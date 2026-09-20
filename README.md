# 第二现场承载协同

服务面向城市赛事第二现场的容量协同，提供可供运营系统探测的基础运行入口。

项目当前开放健康检查接口，便于本地联调与运行巡检。

运行 python3 service.py --check 可核对配置，执行 python3 -m unittest discover -s tests -v 可验证接口契约；使用 python3 service.py --port 8000 启动服务后访问 /health。
