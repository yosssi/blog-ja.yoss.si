package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

// コマンドラインフラグの初期値
const (
	defHost     = "test.mosquitto.org"
	defPort     = "1883"
	defClientID = "mosquitto-test-pub"
	defUsername = ""
	defPassword = ""
	defTopic    = "mosquitto-test"
	defQoS      = 0
	defMsg      = "Hello MQTT"
)

// MQTTブローカ切断時の待ち時間(ms)
const quiesce = 1000

func main() {
	// シグナル通知設定
	chSig := make(chan os.Signal, 1)
	signal.Notify(chSig, os.Interrupt, os.Kill)

	// コマンドラインフラグのパース
	host := flag.String("h", defHost, "MQTTブローカサーバのホスト名")
	port := flag.String("p", defPort, "MQTTブローカサーバのポート番号")
	clientID := flag.String("c", defClientID, "クライアントID")
	username := flag.String("u", defUsername, "認証ユーザ名")
	password := flag.String("pw", defPassword, "認証パスワード")
	topic := flag.String("t", defTopic, "トピック")
	qos := flag.Int("q", defQoS, "QoS")
	msg := flag.String("m", defMsg, "メッセージ")

	flag.Parse()

	// MQTTクライアントの作成
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + *host + ":" + *port)
	opts.SetClientId(*clientID)
	if *username != "" && *password != "" {
		opts.SetUsername(*username)
		opts.SetPassword(*password)
	}

	cli := mqtt.NewClient(opts)

	// ブローカサーバへの接続
	log.Println("ブローカサーバへ接続しています...")
	if _, err := cli.Start(); err != nil {
		panic(err)
	}

	defer func() {
		log.Println("ブローカサーバから切断しています...")
		cli.Disconnect(quiesce)
		log.Println("ブローカサーバから切断しました。")
	}()

	log.Println("ブローカサーバへ接続しました。")

	// Publishの実施
	log.Println("Publishを実施しています...")
	receipt := cli.Publish(mqtt.QoS(*qos), *topic, *msg)
	log.Println("Publishを実施しました。")

	// Publishの実施完了を待つ
	<-receipt
}
