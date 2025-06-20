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
    """确保目录存在，如果不存在则创建"""
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f"创建目录: {dir_path}")


def setup_logging():
    """设置动态命名的日志系统"""
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
        """检查robots.txt是否允许爬取目标页面"""
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
        """获取网页内容，带有重试机制"""
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
        """解析单个电影条目"""
        try:
            # 电影标题
            title_tag = item.find('span', class_='title')
            title = title_tag.text.strip() if title_tag else '无标题'

            # 处理外语片名情况
            title = title.split('/')[0].strip()

            # 导演和基本信息
            info = item.find('div', class_='bd')
            if not info:
                return None

            info_text = info.get_text('|', strip=True)
            info_parts = [part.strip() for part in info_text.split('|') if part.strip()]

            # 提取导演
            director = next((p.replace('导演:', '').strip() for p in info_parts
                             if p.startswith('导演:')), '未知导演')

            # 年份和国家
            year = country = ''
            for part in info_parts:
                year_match = re.search(r'\d{4}', part)
                if year_match:
                    year = year_match.group()
                    country = part.replace(year, '').split('/')[-1].strip()
                    break

            # 评分
            rating_tag = item.find('span', class_='rating_num')
            rating = float(rating_tag.text.strip()) if rating_tag else 0.0

            # 评价人数
            num_tag = item.find('div', class_='star').find_all('span')[-1] if item.find('div', class_='star') else None
            num_str = num_tag.text.replace('人评价', '').strip() if num_tag else '0'
            num = int(num_str.replace(',', ''))

            return {
                'title': title,
                'director': director,
                'year': year,
                'country': country,
                'rating': rating,
                'num': num
            }
        except Exception as e:
            logging.error(f"解析电影失败: {e}\n原始HTML: {str(item)[:200]}...")
            return None

    def scrape(self):
        """执行爬取任务"""
        movies = []
        logging.info("开始爬取豆瓣Top250电影数据")

        # 构建完整的Top250页面URL
        top250_url = BASE_URL + "/top250"

        for start in range(0, MAX_ITEMS, ITEMS_PER_PAGE):
            page_num = start // ITEMS_PER_PAGE + 1
            logging.info(f"正在处理第{page_num}页数据...")

            soup = self._get_page(top250_url, {'start': start})
            if not soup:
                continue

            items = soup.find_all('div', class_='item')
            for item in items:
                movie = self._parse_movie(item)
                if movie:  # 确保有有效数据
                    movies.append(movie)
                    logging.info(f"成功解析电影: {movie['title']}")
                else:
                    logging.warning(f"解析电影失败，跳过该条目")

        logging.info(f"共爬取到{len(movies)}部有效电影数据")
        return movies

    def save_data(self, movies):
        """保存数据到CSV"""
        df = pd.DataFrame(movies)
        df.index += 1  # 编号从1开始

        # 数据清洗
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df = df[df['year'].between(MIN_YEAR, pd.Timestamp.now().year)]
        df['num'] = pd.to_numeric(df['num'], errors='coerce')

        # 确保输出目录存在
        ensure_dir_exists(OUTPUT_CSV)

        df.to_csv(OUTPUT_CSV, encoding='utf-8-sig', index_label='编号')
        logging.info(f"数据已保存到 {OUTPUT_CSV}")

    def analyze(self):
        """数据分析与可视化"""
        try:
            df = pd.read_csv(OUTPUT_CSV, encoding='utf-8-sig')

            # 导演统计TOP5
            director_counts = df['director'].value_counts().head(5)

            # 可视化
            plt.figure(figsize=PLOT_SIZE)
            director_counts.plot(kind='bar', color=BAR_COLOR)

            plt.title('豆瓣Top250电影导演上榜数量TOP5', fontsize=14)
            plt.xlabel('导演', fontsize=12)
            plt.ylabel('电影数量', fontsize=12)
            plt.xticks(rotation=45)

            # 添加数据标签
            for i, v in enumerate(director_counts):
                plt.text(i, v + 0.2, str(v), ha='center')

            # 确保输出目录存在
            ensure_dir_exists(IMAGE_OUTPUT)

            plt.tight_layout()
            plt.savefig(IMAGE_OUTPUT)
            logging.info(f"可视化结果已保存到 {IMAGE_OUTPUT}")

        except Exception as e:
            logging.error(f"数据分析失败: {e}")


if __name__ == '__main__':
    scraper = DoubanScraper()
    movies = scraper.scrape()

    if movies:
        scraper.save_data(movies)
        scraper.analyze()
    else:
        logging.error("未获取到有效数据，程序终止")
