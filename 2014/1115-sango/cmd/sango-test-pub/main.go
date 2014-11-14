package main

import (
	"fmt"
	"os"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

const (
	qos = 0
	msg = "Hello sango"
)

func main() {
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

	// Publishを実施
	receipt := cli.Publish(qos, topic, msg)

	// Publishの実行完了を待つ
	<-receipt
}
