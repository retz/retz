# システムのセットアップ

## 構築の自動化およびシステム設計

一台で構成する場合の Ansible Playbook が `doc/example/centos7.yml` に
ある。これに加えて `retz-server-x.y.z.rpm` および
`retz-client-x.y.z.rpm` をインストールすれば１台構成になる。

Ansible のインストールと環境設定は、次のコマンドで簡単にできる（はず…）。

```
$ sudo yum install gcc python-devel python-virtualenv libffi-devel
$ virtualenv v
$ source v/bin/activate
(v)$ easy_install pip
(V)$ pip install ansible
```

```
$ cat inventory
localhost
$ ansible-playbook centos7.yml -c local -i inventory -b --ask-become-pass
```

と実行すれば、これにRetzが必要な依存ライブラリは全てインストールされる。


Ansible を利用しない場合は、YAMLファイル内の内容を適宜手動で実施する。

クラスタにインストールする場合は各ロールごとにインストールするものを変
えればよい。 `mesos` だけは全てのノードにインストールする必要があるが、
それ以外のものは適宜一部のノードにのみインストールすればよい。


Mesos の動作確認には、

```
$ mesos-execute --command="java -version" --master=localhost:5050 --name=java-version
```

などと適当なコマンドを実行すればよい。結果は、Mesos master の Web UI
`http://localhost:5050` から確認することができる。コンソールに

```
Received status update TASK_RUNNING for task java-version
Received status update TASK_FINISHED for task java-version
```

などと表示されていれば成功である。動作確認ができたら、 Retz のRPMをイ
ンストールする。

```
$ sudo rpm -i retz-server-x.y.z.rpm retz-client-x.y.z.rpm
```

クライアントを利用するときは、 `/opt/retz-client/bin` を `PATH` 環境変数に追加しておく。

```
$ export PATH=/opt/retz-client/bin:$PATH
```

意図した設定が利用されるかどうかは、

```
$ retz-client config
```

を実行して結果を確認すればよい。

### 選択肢0: 直接起動する

1台で動作させる場合は特に設定の変更は必要ない。リモートのMesos master
に接続する場合は必要に応じて、 `retz.md` を参照しながら
`/opt/retz-server/etc/retz.properties` を編集すること。

```
$ /opt/retz-server/bin/retz-server
```

とすれば、フォラグラウンドで動作する。どのようにバックグラウンド動作さ
せ、必要に応じて再起動させるかは運用者の選択肢になる。以下のように、適
当なツールを使えばバックグラウンドでサーバーを動作させることができる。

### 選択肢1: supervisord を使用する

DebianやUbuntu Linux では、 `supervisor` というアプリケーションがあり、
これを利用すればフォアグラウンドで動作するプロセスの起動、再起動、ログ
保存などを定型化することができる。サンプルの設定ファイルが
`/opt/retz-server/etc/retz-server.conf-supervisor-example` としてイン
ストールされるので、これを `/etc/supervisor/conf.d/` に適宜リネームお
よび編集してファイルを設置し、 supervisord をリロードすれば自動的に管
理できるようになる。

TODO: RHELやCentOSでどうなるかの調査

* Pro: 構成、運用がシンプルになる
* Con: ノード故障時にオペレーションが必要

### 選択肢2: Marathon を使用する

`supervisord` を利用した方法では、ノード故障時の対障害性が維持されない。
そこで Marathon にサーバープロセスを管理させる。ノード故障時はMarathon
が別のノードでRetzサーバーのプロセスを起動する。

* Pro: ノード故障時に自動的にフェイルオーバーが行われる
* Con: システムの構成、運用が複雑になる


## Retz が必要なWebサーバー

AsakusaアプリケーションをMesosがダウンロードして環境上に展開できるよう
にするために、MesosがアクセスできるWebサーバーを用意する。たとえば CentOS なら、

```
$ sudo yum install httpd
$ mv asakusa-your-m3bp-application-0.8.1.tar.gz /var/www/html
$ curl -XHEAD http://localhost/asakusa-your-m3bp-applicaiton-0.8.1.tar.gz`
```

としておけば、アプリケーションを以下のコマンドだけで、Mesos/Retz の管理下
で実行し

```
$ retz-client load-app -A your-m3bp-application -F http://localhost/asakusa-your-m3bp-applicaiton-0.8.1.tar.gz
$ retz-client run -A your-m3bp-application -cmd "yaess/bin/yaess-batch.sh ...." -R /tmp
```

とすれば、標準出力などの結果ファイルを `/tmp` に保存できる。この場合、
Tarballは毎回の実行で解凍されるため、サイズが大きい場合には効率が悪い
ので Persistent Volumes を利用する。具体的には、

```
$ retz-client load-app -A your-m3bp-application \
  -P http://localhost/asakusa-your-m3bp-applicaiton-0.8.1.tar.gz \
  -disk 5000
$ retz-client run -A your-m3bp-application -cmd "$RETZ_PVNAME/yaess/bin/yaess-batch.sh ...." \
  -E ASAKUSA_HOME='com.asakusa.m3bp.thread.max=$RETZ_CPU'
  -E YAESS_OPTS='-Xmx:$RETZ_MEMm' -trustpvfiles -R -
```

などとして、アプリケーション読み込み時に `-F` を `-P` で置き換え、その
使用量を `-disk <size>` として、MB単位で指定する。 `-P` で指定したファ
イルは、全て Persistent Volume 上に保存および展開される。 `-R -` は、
標準出力を擬似的にコンソールに出力するオプションである。

`-trustpvfiles` を指定すると、ジョブの実行直前に行う Persistent Volume
上のファイル一覧のチェックを省略する。

# 運用

## cgroups

Mesos が cgroups を操作してくれるので、ユーザー側では上記を設定するだ
けでよい、 Mesos 0.28.2, Linux 4.5/4.6 で確認。デフォルトでは
`cgroups/mesos` で管理


## 監視

* Mesos master - [/state を監視する](http://localhost:5050/state)
* Mesos slave - [/state を監視する](http://localhost:5051/state)
* Retz - [/ping](http://localhost:9090/ping) または [/status を監視する](http://localhost:9090/status)
* (Optional) - ZooKeeper, Marathon

* Disk usage
* システム負荷
* ログ監視

## 障害対応

* 落ちたノードごとの対策
* ネットワークが落ちた場合

### Spark on Mesos

* Set up Mesos cluster, find out where `libmesos.so` is locally
* Download Spark package
* Upload somewhere accessible via `http://` or `hdfs://`
* Not Cluster Mode, but client mode
* set up Marathon;

```
JSON here
```

* Check stdout/stderr from [Mesos Web UI](http://localhost:5050) and make sure it started listening with 7077
* open [SparkDispatcher Web UI](http://localhost:8081)
* run `spark-submit` as follows in cluster mode

* [Runing Spark on Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html)


# FAQ

## Asakusa on M3BP Batch が `execute.sh` が見つからなくて失敗する (FileNotFoundException)

YAESS内で一箇所 `$HOME` にディレクトリ移動する部分があるらしいが詳しい原因は不明。Executor の
子プロセスには正しく設定されているのに…。ワークアラウンドとして、 `schedule` `run` の際に
`-E YAESS_OPTS='-Duser.home=.'` というオプションをつけてJVMに設定を入れることで回避出来る。

## `retz-client run ... -R dir ` がタスク終了後、ファイルのダウンロードで固まる

クライアントがRetzサーバーにはアクセスできるが、Mesos SlaveのHTTP ポー
トにアクセスできないときにこの現象が起きる。
(TODO: TCP connect timeout を短めに設定する)

```
2016-07-14 18:02:33.503 [main] ERROR com.asakusafw.retz.web.Client - Operation timed out
2016-07-14 18:02:33.504 [main] INFO  com.asakusafw.retz.web.Client - Downloading http://172.17.0.2:5051/files/download?path=%2Fvar%2Flib%2Fmesos%2Fslaves%2F0eb15d2f-e376-4d03-a518-f3a6fcbc2ecf-S0%2Fframeworks%2F0eb15d2f-e376-4d03-a518-f3a6fcbc2ecf-0000%2Fexecutors%2Fecho%2Fruns%2F52fcec95-96d9-40f0-8dcd-9186689e64fa/stderr as /Users/kuenishi/src/retz/retz-inttest/build/simple-test-out/stderr
```

**A**. 長期的には改善すべき


## ログが沢山出る

**Q.** `/opt/retz-server/bin/retz-server` を実行したら、 `W0704
  17:29:51.465764 4270 sched.cpp:696] Ignoring framework registered
  message because it was sent from 'master@192.168.100.121:5050'
  instead of the leading master 'master@127.0.0.1:5050'` がむっちゃいっ
  ぱい出続けるんですけど

**A.** Mesos が `0.0.0.0:5050` にバインドしている場合にローカルアドレスで
接続されることを嫌うので、 `retz.properties` で指定している
`retz.mesos = localhost:5050` を変更し Mesos Master のIPアドレスを書く。
