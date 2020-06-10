sql.py 中需要改的参数在代码最开头，主要是输入文件路径、商品数量和本地sql密码三个
data_test.txt是测试矩阵，每行内容为 用户id,推荐度最高商品,次之商品,第三商品
data_test中的数据都为整型常数，方便进行统计商品被推荐次数
data_test规范：第一列用户的id值不能重复
	234列商品的id值不能超出商品id的范围
环境的准备：
	1. sql环境 -> 安装sql后复制MySQL初始化.txt中的代码即可初始化相关数据库和表格
	2. python环境 -> 安装python3.7后 pip install pymysql即可运行

调用说明：
	可以在项目中新建sql类，也可以把sqlpy.java的五行代码直接复制到你的程序里面，修改exe为本地python完整	路径，command为sql.py的完整路径即可
结果检验：
	1. 在命令行中进入MySQL的recommend数据库，执行select * from result;即可看到结果已经存入数据库
	2. 代码运行完毕会自动弹出可视化结果（可视化图中横轴是商品id，纵轴是该商品被推荐的次数）