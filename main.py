"""
豆瓣电影Top250爬虫与分析系统
主要功能：
1. 爬取电影基本信息
2. 存储为结构化CSV文件
3. 进行基础数据分析与可视化
"""

import logging
import os
import random
import re
import time
from datetime import datetime
from urllib.robotparser import RobotFileParser

import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from config import *


def ensure_dir_exists(path):
    """确保目录存在，如果不存在则创建

    Args:
        path (str): 文件或目录路径

    Returns:
        None
    """
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f"创建目录: {dir_path}")


def setup_logging():
    """设置动态命名的日志系统

    Returns:
        str: 日志文件路径
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/{timestamp}.log"

    # 确保日志目录存在
    ensure_dir_exists(log_file)

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )

    # 同时输出到控制台
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    return log_file


class DoubanScraper:
    def __init__(self):
        """初始化爬虫实例，设置日志、检查robots.txt、创建输出目录"""
        # 初始化日志系统
        self.log_file = setup_logging()
        logging.info(f"豆瓣电影爬虫初始化完成，日志文件: {self.log_file}")

        # 检查robots.txt是否允许爬取
        if not self._check_robots_allowed():
            logging.error("根据robots.txt规则，不允许爬取目标页面")
            raise Exception("robots.txt禁止访问目标页面")

        # 确保输出目录存在
        ensure_dir_exists(OUTPUT_CSV)
        ensure_dir_exists(IMAGE_OUTPUT)

        # 设置可视化样式
        plt.style.use(PLOT_STYLE)
        try:
            # 设置中文字体和负号显示
            plt.rcParams['font.sans-serif'] = ['SimSun']
            plt.rcParams['axes.unicode_minus'] = False
        except Exception as e:
            logging.warning(f"中文字体加载失败，将使用默认字体: {str(e)}")

    def _check_robots_allowed(self):
        """检查robots.txt是否允许爬取目标页面

        Returns:
            bool: 是否允许爬取
        """
        try:
            robots_url = BASE_URL + "/robots.txt"
            logging.info(f"正在检查robots.txt: {robots_url}")

            # 获取robots.txt内容
            response = requests.get(
                robots_url,
                headers={'User-Agent': UserAgent().random},
                timeout=TIMEOUT
            )
            response.raise_for_status()

            # 解析robots.txt规则
            rp = RobotFileParser()
            rp.parse(response.text.splitlines())

            # 检查是否允许爬取目标URL
            target_url = BASE_URL + "/top250"
            can_fetch = rp.can_fetch('*', target_url)
            logging.info(f"robots.txt检查结果: {'允许' if can_fetch else '禁止'}爬取 {target_url}")
            return can_fetch

        except Exception as e:
            logging.error(f"robots.txt检查失败: {str(e)}")
            return False  # 默认禁止爬取，以防万一

    def _get_page(self, url, params=None, retry=0):
        """获取网页内容，带有重试机制和反爬策略

        Args:
            url (str): 目标URL
            params (dict, optional): 请求参数
            retry (int, optional): 当前重试次数

        Returns:
            BeautifulSoup: 解析后的页面对象，失败返回None
        """
        try:
            # 随机延迟和动态User-Agent
            time.sleep(random.uniform(*DELAY_RANGE))
            headers = HEADERS.copy()
            headers['User-Agent'] = UserAgent().random

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=TIMEOUT
            )
            response.raise_for_status()  # 自动处理4xx/5xx状态码

            # 检查反爬机制
            if 'accounts.douban.com' in response.url:
                raise requests.exceptions.RequestException("触发反爬机制")

            return BeautifulSoup(response.text, 'html.parser')

        except requests.exceptions.RequestException as e:
            if retry < MAX_RETRIES:
                logging.warning(f"请求失败，第{retry + 1}次重试: {e}")
                return self._get_page(url, params, retry + 1)
            logging.error(f"请求最终失败: {e}")
            return None

    def _parse_movie(self, item):
        """解析单个电影条目

        Args:
            item (bs4.element.Tag): 包含电影信息的HTML标签

        Returns:
            tuple: (电影数据字典, 是否被过滤)
        """
        try:
            # 电影标题
            title_tag = item.find('span', class_='title')
            title = title_tag.text.strip() if title_tag else '无标题'
            title = title.split('/')[0].strip()  # 处理外语片名

            # 导演和基本信息
            info = item.find('div', class_='bd')
            if not info:
                return None, True  # 返回None和过滤标志

            # 提取导演信息
            director_p = info.find('p')
            if director_p:
                director_text = director_p.get_text(strip=True)
                # 提取导演部分，直到遇到" "或结尾
                director_match = re.search(r'导演:(.*?)(?: |$)', director_text)
                if director_match:
                    directors = []
                    for raw_name in director_match.group(1).split('/'):
                        name = raw_name.strip()

                        # 优先提取中文部分
                        chinese_part = re.sub(r'([\u4e00-\u9fff·]+).*', r'\1', name)
                        # 如果没有中文则保留整个名字
                        final_name = chinese_part.strip() if re.search(r'[\u4e00-\u9fff]',
                                                                       chinese_part) else name.strip()

                        if final_name:
                            directors.append(final_name)

                    director = ' / '.join(directors) if directors else '未知导演'
                else:
                    director = '未知导演'

                # 提取年份
                year_match = re.search(r'\d{4}', director_text)
                year = year_match.group() if year_match else ''

                # 国家/地区筛选（模糊匹配）
                p_text = director_p.get_text('\n', strip=True)
                # 找到第一个换行后的内容（年份/国家/类型行）
                lines = p_text.split('\n')
                if len(lines) > 1:
                    country_line = lines[1]  # 例如："1994 / 美国 / 犯罪 剧情"
                    # 找到所有以'/'分隔的元素
                    country_matches = re.findall(r'/\s*([^/\n]+?)\s*/', country_line)
                    if country_matches:
                        country = country_matches[-1].strip()  # 取最后一个匹配项为国家/地区
                        # 如果配置了国家筛选且不匹配任何关键词，则标记为过滤
                        if COUNTRY_FILTER and not any(
                                keyword in country for keyword in COUNTRY_FILTER
                        ):
                            logging.info(f"跳过国家/地区不匹配的电影: {title} ")
                            return None, True
                    else:
                        # 如果没有匹配到国家信息，跳过该电影
                        logging.info(f"无法提取国家/地区信息，跳过电影: {title}")
                        return None, True

            else:
                director = '未知导演'
                year = ''

            # 评分
            rating_tag = item.find('span', class_='rating_num')
            rating = float(rating_tag.text.strip()) if rating_tag else 0.0

            # 评价人数
            num_tag = item.find('span', string=re.compile(r'人评价'))
            if num_tag:
                num_str = re.sub(r'[人评价,]', '', num_tag.text.strip())
                num = int(num_str) if num_str.isdigit() else 0
            else:
                num = 0

            return {
                '中文电影名': title,
                '导演': director,
                '上映时间': year,
                '豆瓣评分': rating,
                '参评人数': num
            }, False  # 返回电影数据和未过滤标志

        except Exception as e:
            logging.error(f"解析电影失败: {e}\n原始HTML: {str(item)[:200]}...")
            return None, True  # 异常情况返回None和过滤标志

    def scrape(self):
        """执行爬取任务

        Returns:
            list: 包含所有电影数据的字典列表
        """
        movies = []
        filtered_count = 0
        seen_titles = set()  # 用于去重的电影名集合
        logging.info("开始爬取豆瓣Top250电影数据")

        # 构建完整的Top250页面URL
        top250_url = BASE_URL + "/top250"

        for start in range(0, MAX_ITEMS, ITEMS_PER_PAGE):
            page_num = start // ITEMS_PER_PAGE + 1
            logging.info(f"正在处理第 {page_num} 页数据...")

            soup = self._get_page(top250_url, {'start': start})
            if not soup:
                continue

            items = soup.find_all('div', class_='item')
            for item in items:
                movie_data, is_filtered = self._parse_movie(item)
                if movie_data:  # 确保有有效数据
                    # 去重检查：基于电影名
                    if movie_data['中文电影名'] in seen_titles:
                        logging.info(f"跳过重复电影: {movie_data['中文电影名']}")
                        filtered_count += 1
                        continue

                    seen_titles.add(movie_data['中文电影名'])
                    movies.append(movie_data)
                    logging.info(f"成功解析电影: {movie_data['中文电影名']}")
                elif not is_filtered:  # 未被主动过滤的解析失败
                    logging.warning(f"解析电影失败，跳过该条目")
                else:
                    filtered_count += 1

        logging.info(f"共爬取到 {len(movies)} 部有效电影数据，过滤了 {filtered_count} 部不符合条件的电影")
        return movies

    def save_data(self, movies):
        """保存数据到CSV文件

        Args:
            movies (list): 包含电影数据的字典列表

        Returns:
            None
        """
        df = pd.DataFrame(movies)
        df.index += 1  # 序号从1开始

        # 数据清洗
        df['上映时间'] = pd.to_numeric(df['上映时间'], errors='coerce')
        df = df[df['上映时间'].between(MIN_YEAR, pd.Timestamp.now().year)]
        df['参评人数'] = pd.to_numeric(df['参评人数'], errors='coerce')

        # 确保输出目录存在
        ensure_dir_exists(OUTPUT_CSV)

        # 保存前再次去重
        df.drop_duplicates(subset=['中文电影名'], keep='first', inplace=True)
        df.to_csv(OUTPUT_CSV, encoding='utf-8-sig', index_label='序号')
        logging.info(f"数据已保存到 {OUTPUT_CSV}")

    def analyze(self):
        """执行数据分析与可视化"""
        try:
            df = pd.read_csv(OUTPUT_CSV, encoding='utf-8-sig')

            # 获取导演统计（包含处理并列情况）
            director_counts = df['导演'].value_counts()

            # 获取第5名的值
            top5_value = director_counts.iloc[4] if len(director_counts) >= 5 else director_counts.min()

            # 获取所有达到或超过第5名值的导演
            top_directors = director_counts[director_counts >= top5_value]

            # 按值降序排序
            top_directors = top_directors.sort_values(ascending=False)

            # 可视化
            plt.figure(figsize=PLOT_SIZE)
            top_directors.plot(kind='bar', color=BAR_COLOR)

            # 构建标题
            title = '豆瓣Top250电影导演上榜数量TOP5+'
            if COUNTRY_FILTER:
                title += f'（国家/地区: {"、".join(COUNTRY_FILTER)}）'

            plt.title(title, fontsize=14)
            plt.xlabel('导演', fontsize=12)
            plt.ylabel('电影数量', fontsize=12)
            plt.xticks(rotation=45 if len(top_directors) > 5 else 0)  # 如果并列多则旋转标签

            # 添加数据标签
            for i, v in enumerate(top_directors):
                plt.text(i, v + 0.2, str(v), ha='center')

            # 确保输出目录存在
            ensure_dir_exists(IMAGE_OUTPUT)

            plt.tight_layout()
            plt.savefig(IMAGE_OUTPUT)
            logging.info(f"可视化结果已保存到 {IMAGE_OUTPUT}")

        except Exception as e:
            logging.error(f"数据分析失败: {e}")


if __name__ == '__main__':
    try:
        scraper = DoubanScraper()
        movies = scraper.scrape()

        if movies:
            scraper.save_data(movies)
            scraper.analyze()
        else:
            logging.error("未获取到有效数据，程序终止")
    except Exception as e:
        logging.error(f"程序运行异常: {e}")
