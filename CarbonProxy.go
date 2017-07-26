package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"strconv"
	"runtime"
	"io/ioutil"
	"net"
	"os/signal"
	"syscall"
	"regexp"
	"github.com/klauspost/crc32"
	//logging "github.com/neko-neko/SocketServer-Example/shared/log"
	logging "log"
	"github.com/neko-neko/SocketServer-Example/server"
	"github.com/c4pt0r/ini"
	"gopkg.in/fatih/pool.v2"
	"github.com/takama/daemon"
)

type connObj struct{
	hostPort string
	connPoolObj pool.Pool
	connErr	error
}

var reHashStatus=false
const MaxPacketQueueSize = 1024
var conf = ini.NewConf("/etc/CarbonProxy.ini")
var serverObj = server.Server{}
var netBufferQueue chan server.PacketQueue
var hashNodeMapLen = 0
var worldUpdateCount int
var regexpStr,regErr = regexp.Compile(`.*\.(?P<urlIndex>[a-z0-9]{32,32})\.[a-z0-9]{32,32}_[0-9]{1,2}\..*`)
var 
(
	cmdList = conf.String("main", "cmdList", "status,rehash,help")
	nodeListStr = conf.String("node","currentNodeList","readfailed")
 	listenPort = conf.Int("main", "port", 5412)
	hostIP =  conf.String("main", "host", "127.0..1")
	threadCount = conf.Int("main", "threadCount", 4)
	leaderPort = conf.Int("main", "leaderPort", 6412)
	checkInterval = conf.Int("main", "checkInterval",60)
	nodemangePort = conf.Int("node","managePort",7412)
	hashSeedFile = conf.String("main","hashSeedFile","/Users/sunshare/SocketServer-Example/CarbonProxy/conf/urlIndex.log.1")
	logFilePath = conf.String("log","accLog","/tmp/CarbonProxy.log")
)

var crc32q = crc32.MakeTable(0xD5828281)
var nodeListMap = make( map[int]connObj,0 )
const (

	// name of the service
	name        = "CarbonProxy"
	description = "Carbon Proxy Hash Server,Carbon Node Manager Server"

	// port which daemon should be listen
	//port = ":9977"
)
var dependencies = []string{"dummy.service"}
type Service struct {
	daemon.Daemon
}
// Manage by daemon commands or run the daemon
func (service *Service) Manage( log logging.Logger) (string, error) {
	conf.Parse()
	initNodeMap(*nodeListStr,log)
 	hashNodeMapLen = len(nodeListMap)
	usage := "Usage:default[/etc/CarbonProxy.ini] myservice install | remove | start | stop | status | checkconfig | help "
	// if received any kind of command, do it
	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return service.Install()
		case "remove":
			return service.Remove()
		case "start":
			return service.Start()
		case "stop":
			return service.Stop()
		case "status":
			return service.Status()
		case "help":
			return usage, nil
		case "checkconfig":
			return checkConfig()
		default:
			return usage, nil
		}
	}

	// Do something, call your goroutines, etc

	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	go reConnect(log)
	//fmt.Print("\"asdfasdf\"")
	// prepare server
	netBufferQueue = make(chan server.PacketQueue, MaxPacketQueueSize)

	serverObj, err := server.NewServer(*hostIP, *listenPort, netBufferQueue)
	if err != nil {
		log.Println("host:",*hostIP,":",*listenPort," create server failed! err(",err,")")
		return usage, nil
	}

	// boot server
	rerr := serverObj.Run()

	if rerr != nil {
		log.Println("host:",*hostIP,":",strconv.Itoa(*listenPort)," start server failed! err(",rerr,")")
		return usage, nil
	}

	// 启动多个处理线程
	i := 0 
	for i < *threadCount - 1{
		go readChannelAndSend(log)
		i = i + 1
	}

	var leaderBufferQueue = make(chan server.PacketQueue)
	leaderServer, err := server.NewServer(*hostIP, *leaderPort, leaderBufferQueue)
	if err != nil {
		log.Println("host:",*hostIP,":",strconv.Itoa(*leaderPort)," create leader server failed! err(",err,")")
		return usage, err
	}else{
		log.Println("host:",*hostIP,":",strconv.Itoa(*leaderPort)," create leader server success! err(",err,")")
	}
	rerr = leaderServer.Run()

	if rerr != nil {
		log.Println("host:",*hostIP,":",strconv.Itoa(*leaderPort)," start leader server failed! err(",rerr,")")
		return usage, rerr
	}else{
		log.Println("host:",*hostIP,":",strconv.Itoa(*leaderPort)," start leader server success! err(",rerr,")")
	}

	timer := time.Tick(100 * time.Millisecond)
	for _ = range timer {
		select {
		// Receive Packet
		case p := <-leaderBufferQueue:

			leaderServer.Notify(p.Connection,[]byte(processCMD(string(p.Packet[:]),log)))

		}
	}

	// never happen, but need to complete code
	return usage, nil
}


func main() {
	conf.Parse()
	//fmt.Print(*hostIP,":",*listenPort,":",*indexFile,":",*nodeListStr)
	runtime.GOMAXPROCS(1)

	if ( *nodeListStr == "readfailed" ){
		fmt.Print("read config file failed!(/etc/CarbonProxy.ini)\n")
		os.Exit(-1)
	}

    logFile, err := os.OpenFile(*logFilePath, os.O_RDWR | os.O_CREATE, 0777)
    if err != nil {
        fmt.Printf("open file error=%s\r", err.Error())
        os.Exit(-1)
    }
	defer logFile.Close()
	var log = logging.New(logFile,"", logging.Ldate | logging.Ltime | logging.Lshortfile)

	//启动节点健康检查 
	srv, err := daemon.New(name, description, dependencies...)
	if err != nil {
		log.Println("Error: ", err)
		os.Exit(1)
	}
	log.Println("asdfdasf")
	service := &Service{srv}
	status, err := service.Manage(*log)
	if err != nil {
		log.Println(status, "\nError: ", err)
		os.Exit(1)
	}
	log.Println(status)
}
func checkConfig()(string,error){

	return "config file is unknow and not complete!",nil
}

func readChannelAndSend(log logging.Logger){
	timer := time.Tick(100 * time.Millisecond)
	for _ = range timer {
		select {
		// Receive Packet
		case p := <-netBufferQueue:

			hashSend(string(p.Packet[:]),log)

		}
	}

}
func initNodeMap( nodeListStr string,log logging.Logger ){
	nodeList := strings.Split(nodeListStr,",")
	for index,value := range nodeList{

		var connObjV = connObj{}
		connObjV.hostPort=value

		var factory = func() (net.Conn, error) { return net.Dial("tcp", connObjV.hostPort) }
		//fmt.Print(error.error())
		connObjV.connPoolObj,connObjV.connErr = pool.NewChannelPool(5, 30, factory)
		//fmt.Print(reflect.TypeOf(p),reflect.TypeOf(err))
		//nodeListMap[index].connPoolObj,nodeListMap[index].connErr := pool.NewChannelPool(5, 30, factory)
		//if tmpMap1["status"] != nil {
		if connObjV.connErr!= nil {
			log.Println("host:",connObjV.hostPort," connect failed! error(",connObjV.connErr,")" )
			//log.Println("host:",tmpMap1["host"]," port:",tmpMap1["port"]," connect failed! error(",tmpMap1["status"].error() )
		}else{
			log.Println("host:",connObjV.hostPort," connect success! msg(",connObjV.connErr,")" )
		}
		nodeListMap[index]=connObjV
		//nodeListMap[index] = tmpMap1
	}
}
func reConnect( log logging.Logger ){
	for {
		for index,value := range nodeListMap{
			if nodeListMap[index].connPoolObj == nil {
				connObjV := value
				var factory = func() (net.Conn, error) { return net.Dial("tcp",connObjV.hostPort) }
				//fmt.Print(error.error())
				connObjV.connPoolObj,connObjV.connErr = pool.NewChannelPool(5, 30, factory)

				if connObjV.connErr != nil {
					log.Println("host:",connObjV.hostPort," reConnect failed! error(",connObjV.connErr,")" )
					//log.Println("host:",tmpMap1["host"]," port:",tmpMap1["port"]," connect failed! error(",tmpMap1["status"].error() )
				}else{
					log.Println("host:",connObjV.hostPort," reConnect success! msg(",connObjV.connErr,")" )

				}
				nodeListMap[index]=connObjV
				//nodeListMap[index] = tmpMap1
			}else{
				log.Println("host:",nodeListMap[index].hostPort," is alived!")
			}
		}
		time.Sleep(time.Duration(*checkInterval)*time.Second)
	}
}

func hashSend( data string,log logging.Logger ){
	lineDataList := strings.Split( data, "" )

	for _, line := range lineDataList{
			urlIndex := regexpStr.FindStringSubmatch(line)
			//fmt.Print( len(urlIndex)," |||| ",line,"")
			if len(urlIndex) == 2  {
				checkSum := crc32.Checksum([]byte(urlIndex[1]),crc32q)
				hashIndex := int(uint32(checkSum)%uint32(hashNodeMapLen))
				if nodeListMap[hashIndex].connPoolObj == nil{
					log.Println("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," is invalid and drop data(",line,")!")
					continue
				}
				conn,err := nodeListMap[hashIndex].connPoolObj.Get()
				if err != nil{
					log.Println("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," get conn error(",err.Error(),") and drop data(",line,")")
					continue
				}
				len,err := conn.Write([]byte(line))
				if err != nil {
					log.Println("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," write error(",err.Error(),")")
				}else{

					log.Println("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," write success(",err,") len=",len," line=\"",line,"\"")
				}
				
			}else{
				log.Println("subThread[hashSend] data format wrong!line=",line,"")
			}
			if regErr != nil {
				log.Println(regErr)
				regErr = nil
			}
	}
	
}


func help(log logging.Logger)(string){
	log.Println("\n-------------Carbon Proxy help----------------")
	log.Println("\t\tCarbon Proxy support cmd:(",*cmdList,")\n")
	return "\t\tCarbon Proxy support cmd:("+*cmdList+")\n"
}
func checkCMD( cmd string,log logging.Logger )(string){
	if !strings.Contains(","+*cmdList+",", ","+cmd+"," ){
		return help(log)
	}
	return  ""
}
func processCMD( _CMD string,log logging.Logger )(string){
	conf.Parse()
	CMD := strings.Replace(_CMD,"\r\n","",-1)
	CMD = strings.Replace(CMD,"\r","",-1)
	CMD = strings.Replace(CMD,"\n","",-1)	

	checkResult := checkCMD(CMD,log)
	if checkResult == "" {
		log.Println("processCMD[subThread]:",CMD)
		switch CMD{
			case "getHashFile":
				return CMD_getHashFileContent()
			case "getNodeList":
				return CMD_getNodeList()
			case "rehash":
				reHashStatus = true
				return "set rehash status is true!"
			case "help":
				return help(log)
			case "getNodeStatus":
				return CMD_getNodeStatus(log)
			default:
				log.Println(CMD)
				return CMD
		}
	}else{
		return checkResult
	}
}
func CMD_getNodeStatus(log logging.Logger)(string){
	retStr := ""
	nodeList := strings.Split(*nodeListStr,",")
	for _,hostPort := range nodeList{
		host := strings.Split( hostPort,":")[0]
		port := *nodemangePort
		retStr = retStr+getNodeStatus(host,port,"getNodeStatus",log)+""
	}
	return retStr
}

func getNodeStatus(host string,port int, cmd string,log logging.Logger)(string){

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Println("connect node(",fmt.Sprintf("%s:%d", host, port),")failed!err(",err,")")
		return fmt.Sprintf("%s:%d", host, port)+" cmd:"+cmd+" msg:"+err.Error()
	}
	defer conn.Close()
	conn.Write([]byte(cmd))
	readBuf := make([]byte, 1024)
	readLen, _ := conn.Read(readBuf)

	return fmt.Sprintf("%s:%d", host, port)+" cmd:"+cmd+" msg:"+ string(readBuf[:readLen])
}
func CMD_getNodeList()(string){
	return *nodeListStr
}

func CMD_getHashFileContent()(string){
	file := *hashSeedFile
	b, e := ioutil.ReadFile(file)
	if e != nil {
		return "hash seed file("+file+") read failed!"
	}
	return string(b)
}