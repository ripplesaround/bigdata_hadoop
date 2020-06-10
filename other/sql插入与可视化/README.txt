sql.py 中需要改的参数在代码最开头，主要是输入文件路径、商品数量和本地sql密码
data_test.txt是测试矩阵，每行内容为 用户id,推荐度最高商品,次之,第三
	data_test中的数据都为整型常数，方便进行统计商品被推荐次数
	规范：用户列的id值不能重复
		商品列的id值不能超出商品个数-1
环境的准备：
	1. sql环境 -> 安装sql后复制MySQL初始化.txt中的代码即可初始化相关数据库和表格
	2. python环境 -> 安装python3.7后 pip install pymysql即可运行
	3. java集成环境 -> 需要安装jython，安装网址http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.7.0/jython-installer-2.7.0.jar，下载后cd到目标文件夹执行：java -jar jython_installer-2.7.0.jar即可打开安装程序，一路yes即可安装，中间有一个选standard即可