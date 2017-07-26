carbon-proxy功能：
1.依据carbon数据32位字符串hash发送到后端carbon节点
2.实现carbon节点rehash功能
安装流程:
go build
go install
/usr/local/go/bin/CarbonProxy install
/usr/local/go/bin/CarbonProxy start
cp ./conf/conf.ini /etc/CarbonProxy.ini