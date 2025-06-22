# 爬虫基本配置
BASE_URL = "https://movie.douban.com"
DELAY_RANGE = (1, 3)  # 随机延迟范围(秒)
TIMEOUT = 10  # 请求超时时间(秒)
MAX_RETRIES = 3  # 失败重试次数

# 请求头配置
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Connection': 'keep-alive',
    'Referer': 'https://www.douban.com/',
}

# 文件配置
OUTPUT_CSV = 'data/film_name.csv'  # 数据输出文件
IMAGE_OUTPUT = 'data/director_top5.png'  # 可视化结果输出

# 可视化配置
PLOT_STYLE = 'ggplot'  # matplotlib样式
PLOT_SIZE = (10, 6)  # 图表尺寸(宽, 高)
BAR_COLOR = '#3498db'  # 柱状图颜色

# 数据解析配置
COUNTRY_FILTER = ['中国']  # 国家/地区筛选配置（可选）
MIN_YEAR = 1900  # 最小有效年份
MAX_ITEMS = 250  # 最大爬取数量
ITEMS_PER_PAGE = 25  # 每页项目数
