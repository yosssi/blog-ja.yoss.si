package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

const qos = 0

// handleはメッセージ受信時の処理を実施する。
func handle(_ *mqtt.MqttClient, msg mqtt.Message) {
	fmt.Printf("Topic: %s\nMessage: %s\n", msg.Topic(), msg.Payload())
}

func main() {
	// シグナル通知設定
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	// 引数チェック
	if len(os.Args) != 5 {
		fmt.Println("ブローカサーバのURI、ユーザ名、パスワード、トピックを引数に指定してください。")
		os.Exit(1)
	}

	// 変数の設定
	uri := os.Args[1]
	name := os.Args[2]
	pw := os.Args[3]
	topic := os.Args[4]

	// MQTTクライアントの作成
	opts := mqtt.NewClientOptions()
	opts.AddBroker(uri)
	opts.SetUsername(name)
	opts.SetPassword(pw)

	cli := mqtt.NewClient(opts)

	// ブローカサーバへの接続
	if _, err := cli.Start(); err != nil {
		panic(err)
	}

	defer cli.Disconnect(1000)

	// フィルタの設定
	filter, err := mqtt.NewTopicFilter(topic, qos)
	if err != nil {
		panic(err)
	}

	// Subscriptionの開始
	if _, err := cli.StartSubscription(handle, filter); err != nil {
		panic(err)
	}

	defer func() {
		receipt, err := cli.EndSubscription(topic)
		if err != nil {
			panic(err)
		}

		<-receipt
	}()

	// 割り込み発生まで待つ
WaitLoop:
	for {
		select {
		case <-c:
			break WaitLoop
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
