# Grafana安装
Grafana下载地址：https://grafana.com/grafana/download

版本：4.4.1

选择相应的操作系统下载并安装

# 数据源插件安装
基于simple-json-datasource数据源插件连接IoTDB数据库。

插件下载地址：https://github.com/grafana/simple-json-datasource

下载并解压，将文件放到Grafana的目录中：
`data/plugin/`（Windows）或`/var/lib/grafana/plugins` (Linux)

# 启动Grafana
启动 Grafana

# IoTDB安装
参考：https://github.com/thulab/iotdb

# 后端数据源连接器安装
下载源代码
```
git clone git@github.com:thulab/iotdb-grafana.git
```
进入目录，打成war包
```
mvn clean package
```
将`application.properties`文件从`conf/`目录复制到`target`目录下，并编辑属性值
```
spring.datasource.url = jdbc:tsfile://127.0.0.1:6667/
spring.datasource.username = root
spring.datasource.password = root
spring.datasource.driver-class-name=cn.edu.tsinghua.iotdb.jdbc.TsfileDriver
server.port = 8888
```

采用IoTDB作为后端数据源，前四行定义了数据库的属性，默认端口为6667，用户名和密码都为root，指定数据源驱动的名称。

编辑server.port的值修改连接器的端口。

# 运行启动

启动数据库，参考：https://github.com/thulab/iotdb

运行后端数据源连接器，在控制台输入
```$xslt
cd target/
java -jar iotdb-grafana-0.7.0.war
```
Grafana的默认端口为 3000，在浏览器中访问 http://localhost:3000

用户名和密码都为 admin

# 添加数据源
在首页点击左上角的图标，选择`Data Sources`，点击右上角`Add data source`图标，填写`data source`相关配置，在`Config`中`Type`选择`SimpleJson`，`Url`填写http://localhost:8888

端口号和数据源连接器的端口号一致，填写完整后选择`Add`，数据源添加成功。

# 设计并制作仪表板
在首页点击左上角的图标，选择`Dashboards` - `New`，新建仪表板。在面板中可添加多种类型的图表。

以折线图为例说明添加时序数据的过程：

选择`Graph`类型，在空白处出现无数据点的图，点击标题选择`Edit`，在图下方出现属性值编辑和查询条件选择区域，在`Metrics`一栏中`Add Query`添加查询，点击`select metric`下拉框中出现IoTDB中所有时序的名称，在右上角选择时间范围，绘制出对应的查询结果。可设置定时刷新，实时展现时序数据。

