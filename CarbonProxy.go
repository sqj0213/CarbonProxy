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
	//"encoding/json"
	"github.com/klauspost/crc32"
	//"github.com/Sirupsen/logrus"
	"github.com/neko-neko/SocketServer-Example/shared/log"
	"github.com/neko-neko/SocketServer-Example/server"
	"github.com/c4pt0r/ini"
	"gopkg.in/fatih/pool.v2"
	"github.com/takama/daemon"
)

type connObj struct{
	hostPort string
	writeCount int
	writeErrorCount int
	readCount int
	readErrorCount int
	connPoolObj pool.Pool
	connErr	error
}
type daemonStatus struct{
	invalidDataCount int
	inCount int
	sendCount int
}
var mainStatus = daemonStatus{}

var reHashStatus=false
const MaxPacketQueueSize = 1024*1024*10
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
var nodeListMap = make( map[int]*connObj,0 )
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
func (service *Service) Manage( ) (string, error) {


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

	go reConnect(true)
	//fmt.Print("\"asdfasdf\"")
	// prepare server
	netBufferQueue = make(chan server.PacketQueue, MaxPacketQueueSize)

	serverObj, err := server.NewServer(*hostIP, *listenPort, netBufferQueue)
	if err != nil {
		log.Info("host:",*hostIP,":",*listenPort," create server failed! err(",err,")")
		return usage, nil
	}

	// boot server
	rerr := serverObj.Run()

	if rerr != nil {
		log.Info("host:",*hostIP,":",strconv.Itoa(*listenPort)," start server failed! err(",rerr,")")
		return usage, nil
	}

	// 启动多个处理线程
	i := 0 
	for i < *threadCount - 1{
		go readChannelAndSend()
		i = i + 1
	}

	var leaderBufferQueue = make(chan server.PacketQueue)
	leaderServer, err := server.NewServer(*hostIP, *leaderPort, leaderBufferQueue)
	if err != nil {
		log.Info("host:",*hostIP,":",strconv.Itoa(*leaderPort)," create leader server failed! err(",err,")")
		return usage, err
	}else{
		log.Info("host:",*hostIP,":",strconv.Itoa(*leaderPort)," create leader server success! err(",err,")")
	}
	rerr = leaderServer.Run()

	if rerr != nil {
		log.Info("host:",*hostIP,":",strconv.Itoa(*leaderPort)," start leader server failed! err(",rerr,")")
		return usage, rerr
	}else{
		log.Info("host:",*hostIP,":",strconv.Itoa(*leaderPort)," start leader server success! err(",rerr,")")
	}

	//timer := time.Tick(10 * time.Millisecond)
	//for _ = range timer {
	for {
		select {
		// Receive Packet
		case p := <-leaderBufferQueue:

			leaderServer.Notify(p.Connection,[]byte(processCMD(string(p.Packet[:]))))

		}
	}

	// never happen, but need to complete code
	return usage, nil
}


func main() {
mainStatus.invalidDataCount=0
mainStatus.inCount=0
mainStatus.sendCount=0
var rlim syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err != nil {
		fmt.Println("get rlimit error: " + err.Error())
		os.Exit(1)
	}
	rlim.Cur = 50000
	rlim.Max = 50000
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err != nil {
		fmt.Println("set rlimit error: " + err.Error())
		os.Exit(1)
	}
	conf.Parse()

	initNodeMap(*nodeListStr)
 	hashNodeMapLen = len(nodeListMap)
	reConnect(false)

	//fmt.Print(*hostIP,":",*listenPort,":",*indexFile,":",*nodeListStr)
	runtime.GOMAXPROCS(*threadCount)

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
	log.SetOut(logFile)
	//log.logger.Level=logrus.InfoLevel
	//log.SetLevel()

	//启动节点健康检查 
	srv, err := daemon.New(name, description, dependencies...)
	if err != nil {
		log.Info("Error: ", err)
		os.Exit(1)
	}
	log.Info("asdfdasf")
	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		log.Info(status, "\nError: ", err)
		os.Exit(1)
	}
	log.Info(status)
}
func checkConfig()(string,error){

	return "config file is unknow and not complete!",nil
}

func readChannelAndSend(){
	timer := time.Tick(1 * time.Millisecond)
	for _ = range timer {
		select {
		// Receive Packet
		case p := <-netBufferQueue:

			hashSend(string(p.Packet))
			log.Info("recivedmsg:",string(p.Packet))

		}
	}

}
func initNodeMap( nodeListStr string ){
	nodeList := strings.Split(nodeListStr,",")
	for index,value := range nodeList{

		var connObjV = connObj{}
		connObjV.hostPort=value
		connObjV.writeCount = 0
		connObjV.readCount = 0
		connObjV.writeErrorCount = 0
		var factory = func() (net.Conn, error) { return net.Dial("tcp", connObjV.hostPort) }
		//fmt.Print(error.error())
		connObjV.connPoolObj,connObjV.connErr = pool.NewChannelPool(5, 30, factory)
		//fmt.Print(reflect.TypeOf(p),reflect.TypeOf(err))
		//nodeListMap[index].connPoolObj,nodeListMap[index].connErr := pool.NewChannelPool(5, 30, factory)
		//if tmpMap1["status"] != nil {
		if connObjV.connErr!= nil {
			log.Info("host:",connObjV.hostPort," connect failed! error(",connObjV.connErr,")" )
			//log.Info("host:",tmpMap1["host"]," port:",tmpMap1["port"]," connect failed! error(",tmpMap1["status"].error() )
		}else{
			log.Info("host:",connObjV.hostPort," connect success! msg(",connObjV.connErr,")" )
		}
		nodeListMap[index]=&connObjV
		//nodeListMap[index] = tmpMap1
	}
}
func reConnect( checkFlag bool ){
	for {
		for index,value := range nodeListMap{
			if nodeListMap[index].connPoolObj == nil {
				connObjV := value
				connObjV.writeCount = 0
				connObjV.writeErrorCount = 0
				connObjV.readCount = 0
				connObjV.readErrorCount = 0
				var factory = func() (net.Conn, error) { return net.Dial("tcp",connObjV.hostPort) }
				//fmt.Print(error.error())
				connObjV.connPoolObj,connObjV.connErr = pool.NewChannelPool(5, 30, factory)

				if connObjV.connErr != nil {
					log.Info("host:",connObjV.hostPort," reConnect failed! error(",connObjV.connErr,")" )
					//log.Info("host:",tmpMap1["host"]," port:",tmpMap1["port"]," connect failed! error(",tmpMap1["status"].error() )
				}else{
					log.Info("host:",connObjV.hostPort," reConnect success! msg(",connObjV.connErr,")" )

				}
				nodeListMap[index]=connObjV
				//nodeListMap[index] = tmpMap1
			}else{
				log.Info("host:",nodeListMap[index].hostPort," is alived!")
			}
		}
		if ( checkFlag ){
			time.Sleep(time.Duration(*checkInterval)*time.Second)
		}else{
			break
		}
	}
}

func hashSend( data string ){
	lineDataList := strings.Split( data, "\n" )

	for _, line := range lineDataList{
			urlIndex := regexpStr.FindStringSubmatch(line)
			mainStatus.inCount = mainStatus.inCount + 1
			//fmt.Print( len(urlIndex)," |||| ",line,"")
			line=line+"\n"
			if len(urlIndex) == 2  {
				checkSum := crc32.Checksum([]byte(urlIndex[1]),crc32q)
				hashIndex := int(uint32(checkSum)%uint32(hashNodeMapLen))
				if nodeListMap[hashIndex].connPoolObj == nil{
					log.Info("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," is invalid and drop data(",line,")!")
					continue
				}
				conn,err := nodeListMap[hashIndex].connPoolObj.Get()
				if err != nil{
					nodeListMap[hashIndex].writeErrorCount = nodeListMap[hashIndex].writeErrorCount + 1
					//nodeListMap[hashIndex].writeCount = nodeListMap[hashIndex].writeCount + 1
					log.Info("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," get conn error(",err.Error(),") and drop data(",line,")")
					continue
				}

				len,err := conn.Write([]byte(line))
				if err != nil {
					nodeListMap[hashIndex].writeErrorCount = nodeListMap[hashIndex].writeErrorCount + 1
					log.Info("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," write error(",err.Error(),")")
				}else{
					mainStatus.sendCount = mainStatus.sendCount + 1
					log.Info("subThread[hashSend] host:",nodeListMap[hashIndex].hostPort," write success(",err,") len=",len," line=\"",line,"\"")
				}
				fmt.Print(nodeListMap[hashIndex].writeCount )
				//nodeListMap[hashIndex].writeCount = nodeListMap[hashIndex].writeCount + 1
				
			}else{
				mainStatus.invalidDataCount = mainStatus.invalidDataCount + 1
				log.Info("subThread[hashSend] data format wrong!line=",line,"")
			}
			if regErr != nil {
				log.Info(regErr)
				regErr = nil
			}
	}
	
}


func help()(string){
	log.Info("\n-------------Carbon Proxy help----------------")
	log.Info("\t\tCarbon Proxy support cmd:(",*cmdList,")\n")
	return "\t\tCarbon Proxy support cmd:("+*cmdList+")\n"
}
func checkCMD( cmd string )(string){
	if !strings.Contains(","+*cmdList+",", ","+cmd+"," ){
		return help()
	}
	return  ""
}
func processCMD( _CMD string )(string){
	CMD := strings.Replace(_CMD,"\r\n","",-1)
	CMD = strings.Replace(CMD,"\r","",-1)
	CMD = strings.Replace(CMD,"\n","",-1)	

	checkResult := checkCMD(CMD)
	if checkResult == "" {
		log.Info("processCMD[subThread]:",CMD)
		switch CMD{
			case "getHashFile":
				return CMD_getHashFileContent()
			case "getNodeList":
				return CMD_getNodeList()
			case "status":
				return CMD_getDaemonStatus()
			case "rehash":
				reHashStatus = true
				return "set rehash status is true!"
			case "help":
				return help()
			case "getNodeStatus":
				return CMD_getNodeStatus()
			default:
				log.Info(CMD)
				return CMD
		}
	}else{
		return checkResult
	}
}
func CMD_getDaemonStatus()(string){
	var tmpStr="hostPort:__hostPort__|writeCount:__writeCount__|writeErrorCount:__writeErrorCount__|readCount:__readCount__|readErrorCount:__readErrorCount__\n"
	var tmplStr="inCount:__inCount__,sendCount:__sendCount__,invalidDataCount:__invalidDataCount__,rehashStatus:__rehashStatus__||__LINELIST__"
	var retStr=""
	var lineStr=""
	retStr = strings.Replace(tmplStr,"__invalidDataCount__",strconv.Itoa(mainStatus.invalidDataCount),-1)
	retStr = strings.Replace(retStr,"__inCount__",strconv.Itoa(mainStatus.inCount),-1)
	retStr = strings.Replace(retStr,"__sendCount__",strconv.Itoa(mainStatus.sendCount),-1)
	if reHashStatus {
		retStr = strings.Replace(retStr,"__rehashStatus__","true",-1)
	}else{
		retStr = strings.Replace(retStr,"__rehashStatus__","false",-1)
	}
	for _,value := range nodeListMap{
		lineStr = lineStr+strings.Replace(tmpStr,"__hostPort__",value.hostPort,-1)
		lineStr = strings.Replace(lineStr,"__writeCount__",strconv.Itoa(value.writeCount),-1)
		lineStr = strings.Replace(lineStr,"__writeErrorCount__",strconv.Itoa(value.writeErrorCount),-1)
		lineStr = strings.Replace(lineStr,"__readCount__",strconv.Itoa(value.readCount),-1)
		lineStr = strings.Replace(lineStr,"__readErrorCount__",strconv.Itoa(value.readErrorCount),-1)
	}
	retStr = strings.Replace(retStr,"__LINELIST__",lineStr,-1)
	fmt.Print(retStr)
	return string(retStr)
}

func CMD_getNodeStatus()(string){
	retStr := ""
	nodeList := strings.Split(*nodeListStr,",")
	for _,hostPort := range nodeList{
		host := strings.Split( hostPort,":")[0]
		port := *nodemangePort
		retStr = retStr+getNodeStatus(host,port,"getNodeStatus")+""
	}
	return retStr
}

func getNodeStatus(host string,port int, cmd string)(string){

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Info("connect node(",fmt.Sprintf("%s:%d", host, port),")failed!err(",err,")")
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
