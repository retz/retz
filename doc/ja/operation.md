# システムのセットアップ

## 構築の自動化およびシステム設計

一台で構成する場合の Ansible Playbook が `doc/example/centos7.yml` に
ある。これに加えて `retz-server-x.y.z.rpm` および
`retz-client-x.y.z.rpm` をインストールすれば１台構成になる。

Ansible のインストールと環境設定は、次のコマンドで簡単にできる（はず…）。

```
$ sudo yum install gcc python-devel python-virtualenv libffi-devel openssl-devel
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

`load-app` はアプリケーション、ジョブの実行環境をRetzに登録する。拡張
子が圧縮ファイルのものである場合はMesos Agent がこれを展開する。 `hdfs://` 
をスキームとして利用すると、HDFS上のファイルも取得することができる。ただし、
`hadoop` コマンドが Mesos Agent および Executor から利用可能な状況に
なっていなければならない。


* アプリケーションの実行環境を用意する方法

Mesos Containerは、何も指定しなければ何のファイルもない。ジョブを実行
するために実行環境（独自の実行ファイルや、 Executable Jar など）をコン
テナ内に用意しなければならない。また、入力データをローカルファイルとし
てアクセス可能な場所に用意したり、出力データを保存するストレージが必要
がある。

1. `-F` オプションを使って、Mesosが実行時にダウンロードおよび展開する
* [Mesos Fetcher](http://mesos.apache.org/documentation/latest/fetcher/)
* これらのアプリケーション情報やデータはMesosスレーブ上に保存される。
Executor が終了してからしばらくは残るが、GC時に削除される。
* Pro: HDFS/S3 など多様なプロトコルに対応している
* Con: ジョブ実行時に毎回解凍するので、あまり大きいと実行時間に影響する

2. `-L` オプションを使って、Mesosが実行時にダウンロードおよび展開する
* ファイルがMesos agent 上でキャッシュされる以外は、 `-F` と同様

3. `-P` オプションを使って、 アプリケーション開始時に永続領域を確保し、
  そこにファイルを展開する (Removed)
* [Persistent Volumes](http://mesos.apache.org/documentation/latest/persistent-volume/)
* データはRetzがアプリケーション用に確保したAgent上の永続領域に保存さ
  れる。これは、Executorの実行ディレクトリ下に `retz-<app-name>` とい
  う名前で保存および展開される。展開には通常の `tar` コマンドを用いて
  いる。
* `-disk` オプションで、 **必ず** 確保する永続領域のサイズを宣言する。
  このオプションは必須。この上限を超えたときの挙動は、 Agent が動作し
  ているOSおよびファイルシステムの挙動に従う。
* このPersistent VolumeはアプリケーションをUnloadした後、およびRetzを
  再起動した後に開放される。
* Pro: 永続化されており、入出力データの受け渡しにも使える（かもしれない、要検証）
* Con: ひとつのVolumeについては、同時に1つしかジョブを実行することができない

4. `--container` オプションを使って、コンテナ上で実行する
* [Containerizers](http://mesos.apache.org/documentation/latest/containerizer/)
* Mesos Agent に `--containerizers=docker` 等の記入が必要
* `--container docker --image ubuntu:latest [--argv a,b,c,d] [--force-pull]` と指定
* Pro: Dockerレポジトリに動作を依存することになる。独自レポジトリを管理し、 Mesos
  Agent の引数に設定する必要がある。
* Con: `-F` `-L` 等でファイルを解凍するときのオーバーヘッドがなく、 `-P` のように
  複雑なライフサイクルを考慮する必要がない

ジョブが投入され、リソースが確保されると、 Mesos Agentが必要なファイル
を指定された場所から取得し、 Retz Executor が Mesos Agent から `fork,
exec` される。その結果は切り離されたディレクトリに保存される。

Executor ID はアプリケーションIDが利用されているので、Mesos masterのコ
ンソールから確認できる。

Executor のライフサイクル: Executor はデータを再利用するため、タスク終
了後のプロセスは残る。Executor を再利用する。Executor は `stop`しなけ
ればその場に残り続けて、次の `launchTask` を待つ。逆に環境をリセットし
たい場合は、このExecutorを止めればよい。これは `unload-app` するまで残
る可能性がある（場合によっては自動的にMesosによってタスクがないときに
停止される）。


#### その他のジョブ管理

```sh
$ /opt/retz-client/bin/retz-client [-C <configfile>] <command> [command args]
```

設定ファイルはデフォルトでは `/opt/retz-client/etc/retz.properties` を参照する。

#### 使用例

```sh
$ retz-client list
$ retz-client kill -id <jobid>
$ retz-client schedule OPTIONS
$ retz-client run OPTIONS
$ retz-client schedule -file <list of batches in a text file>
$ retz-client get-job -id <jobid>
$ retz-client get-file -id <jobid> --fetch <filename> --poll -R -
$ retz-client list-files -id <jobid> --path <path> -R -
$ retz-client config
```

`run` `schedule` のオプション `OPTIONS`


* `-cmd 'yaess/bin/yaess-batch.sh ...'` [must] 実際にリモートで起動されるYAESSのコマンド。
* `-A --application your-app-name ` [opt] 登録したアプリケーション名。
  これがない場合は、 `CommandExecutor` が使用される。
* `-E key=value` 環境変数を設定する。
* `-R <directory>` 標準出力などのファイルをローカルにコピーするディレクトリを指定する（ `get-file` のみ）
* `-N --name <jobname>` [opt default=retz-task-name-<app-name>]
* `-cpu <posint>-[<posint>]` [opt default=1-] ジョブを実行可能なCPU数の範囲。ジョ
  ブは通常、Mesosに与えられたCPU数または、ここで指定した最大値を実行時に設定する。
* `-mem <posint>-[<posint>]` [opt default=512-] in MB ジョブを実行可能なメモリ量の範囲。ジョ
  ブは通常、Mesosに与えられたRAM量または、ここで指定した最大値を実行時に利用する。
* `-gpu <posint>` [opt default=0] ジョブを実行可能なGPUの枚数。
  これを有効にするためには、サーバーの設定ファイルで `retz.gpu=true` を設定しておかなければならない。
* `-id <jobid>` ジョブの状態を表示する。`-R <dir>` を `run` と同様に指定することで、結果をローカルに取得できる
* `-file <filename>` [must if batch/home is omitted] **未実装**
* `-retry <posint>` [opt default=config] **未実装**

`-E key=value` の形で環境変数を指定するときに、Executor上での実行環境に合わせた値を利用できる。

例えば、 Asakusa on M3BP で利用する場合は

```sh
$ retz-client schedule -A asakusa \
  -E ASAKUSA_M3BP_ARGS='--engine-conf com.asakusafw.m3bp.thread.max=$RETZ_CPU' \
  -E ASAKUSA_M3BP_OPTS='-Xmx$RETZ_MEMm' -E ASAKUSA_HOME='$RETZ_PVNAME' \
     -cmd '$RETZ_PVNAME/yaess/bin/yaess-batch.sh m3bp.example.summarizeSales -A date=2011-04-01'
```

などとする。このとき注意するのは、環境引数の指定を `'` を使ってシング
ルクォートすることである。そうしなければ、ローカルのシェルで
`$RETZ_...` などの変数が評価されてしまう。

* **Known Issue** YAESSを使ってホームディレクトリからの相対パスで読ま
  せる場合は、 `-E YAESS_OPTS='-Duser.home=.'` などとカレントディレク
  トリを指定すること。理由は不明だが、 `$HOME` が指定されても無視され
  ることがわかっている。 (Mesos がいくつかの Containerizer 上で `HOME` を
  上書きしている模様)

#### 設定ファイル

`retz.properties` という名前を推奨する。デフォルトではサーバー用は
`/opt/retz-server/etc` クライアント用は `/opt/retz-client/etc` にイン
ストールされる。

```
retz.mesos = hostname:5050
retz.mesos.offer_interval = 1
retz.gpu=false
retz.bind  = http://my-hostname:9090
retz.queue.max = 65536
retz.key = deadbeef
retz.secret = cafebabe
retz.schedule.results = /path/to/dir/
retz.schedule.retry = 5
retz.tls.keystore.file = path/to/retz-dev.jks
retz.tls.keystore.pass = retz-dev
retz.tls.insecure = true
```

いま有効なのは以下のものである:

* `retz.mesos` - Mesos master の位置を `master.mesos.example.com:5050` などと指定する。省略不可
* `retz.mesos.offer_interval` - Mesos からの resource offer を受け取る間隔を指定する。デフォルトは5。単位は秒
* `retz.gpu` - Mesos に対して GPU_RESOURCES Capability を指定する
* `retz.mesos.role` - Mesos 上でのRetzのRoleを指定する。デフォルトは `*`
* `retz.mesos.principal` - Mesos 上でのRetzのPrincipalを指定する。デフォルトは空
* `retz.bind` - Retz が listen するTCPのポート番号およびアドレスを `http://localhost:9090` などと指定する。省略不可
* `retz.tls.keystore.file`
* `retz.tls.keystore.pass`
* `retz.tls.insecure`

これらが正しく設定されているかどうかは、 `retz-client -C
path/to/retz.properties config` を実行する。

#### ジョブリスト（未実装）

ジョブをまとめて投入したい場合、 `retz-client schedule` コマンドをルー
プで繰り返す方法は、 WebSocket コネクションを毎回接続するので非効率で
ある。ひとつの WebSocket コネクションで一度にジョブを投入するためには、
`-file <filename>` オプションを使用する。ファイルは1行に各コマンドを記
述する。各行は

```
-A appName -cmd 'yaess/bin/yaess-batch.sh example.appName'
```

といったふうに、 `retz-client schedule` の引数を列挙する。

TODO: ワークフローとして定義できるようにした方がよいかもしれない


##### 基本方針

Asakusa on M3BP ジョブは、なるべく少ない並列数で実行しつつ、複数のジョブを並列で
実行するのが全体のスループットは効率がよい。これは個々のジョブのレイテ
ンシとのトレードオフなので、各ジョブの並列数を調整できるようにする。具
体的には以下の通りcgroups を利用してジョブがCPUを使いすぎないようにす
る（NUMAノードをはみ出さないようにする）retz でNUMAノードの割当を管理
するm3bp は、割り当てられたNUMAノードだけを利用してジョブを実行する

##### やること

* Mesos slave に `--resources="numanodes:2"` を設定し、NUMA ノードを管
  理できるようにするCPU利用率にハードリミットをつけNUMAノードを管理で
  きるようにする
* CPU pinning on NUMA nodes
* CPU pinning by M3BP
* Asakusa on M3BP / libm3bp に引数 `ASAKUSA_M3BP_ARGS='--engine-conf
  c.a.m.t.affinity="@0,1"'` などと割り当てられたNUMA nodeに
  sched_setaffinity できるようにする
* 現状、M3BP は PUid 0 番から順番に利用可能なところにスレッド固定をす
  るため、M3BPプロセスが同時起動すると意図しない配置になる場合がある



#### スケジューリング

M3BP はソケット単位で CPU Affinityを設定する。
`com.asakusafw.m3bp.thread.max` を設定すれば自動的にスレッド数を固定し
てくれるので、それ以上コアを専有することはない。8コアマシンで6:2 でス
レッド数が指定された場合の固定は hwloc がうまく Affinity を回してくれ
なくて効率が悪いときは `com.asakusafw.m3bp.thread.affinity` を`none`に
してみるとOS任せになってうまく動く（かもしれない）。

* [MESOS-314](https://issues.apache.org/jira/browse/MESOS-314) によれ
  ば、まだCPU pinning はできないので、OSに任せることとする
* [dockerのcpusetを使えばできるかも](https://theza.ch/2014/09/17/cpu-resources-in-docker-mesos-and-marathon/) ... 要検証、優先度低

#### HTTP プロトコル


`Authorization` ヘッダに認証情報を記述できる。 `retz.access.key` と
`retz.access.secret` を用いた署名を組み合わせて、サーバーとクライアントで
署名を検証する。

* `GET /job/:id` behind `get-job` subcommand

* `GET /job/:id/file/:file` behind `get-file` subcommand

* `GET /job/:id/path/:path` behind `list-files` subcommand

* `PUT /job` behind `schedule` `run` subcommand

Request: `{"command":"schedule", "job":Job, "doWatch":false}`
Response: `{"status":"ok", "job":Job1}` or `{"status":"queue full"}`

* `DELETE /job/:id` behind `kill` subcommand

Request: `{"command":"kill", "id":24}`
Response: `{"status":"ok"}` or `{"status":"not found"}`

* `GET /jobs` behind `list` command

Request: `{"command": "list"}`
Response: `{"status":"ok", "queue": [ Job0, Job1, .... ]}`

* `GET /apps` behind `list-app` command
* `PUT /app/:name` behind `load-app` command
* `DELETE /app/:name` behind `unload-app` command

* `Job`

```
{
 "cmd":"yaess/bin/yaess-batch.sh ....",
 "scheduled":"2016-05-15:20:20:20Z",
 "started":"2016-05-15:20:20:20Z",
 "finished":"2016-05-15:20:20:20Z",
 "result":0,
 "id":0,
 "url":"https://....",
 ...
}
```


Event: `started` `killed` `scheduled` `finished`


# FAQ

## Q. Dockerized application で `Message from Mesos executor: Abnormal executor termination` が出る

Mesos Agent の `containerizers` オプションに `docker` が含まれていない。
`/etc/mesos-slave/containerizers` に `mesos,docker` などと書くとよい。

## Q. `pip install ansible` で `ImportError: No module named markupsafe` が出る

[Jinja2](https://github.com/ansible/ansible/issues/13570) の問題と思われる
が、再現条件がはっきりしていない。大抵は `pip install markupsafe` で解決する。

## Q. Asakusa on M3BP Batch が `execute.sh` が見つからなくて失敗する (FileNotFoundException)

YAESS内で一箇所 `$HOME` にディレクトリ移動する部分があるらしいが詳しい原因は不明。Executor の
子プロセスには正しく設定されているのに…。ワークアラウンドとして、 `schedule` `run` の際に
`-E YAESS_OPTS='-Duser.home=.'` というオプションをつけてJVMに設定を入れることで回避出来る。

## Q. `retz-client run ... -R dir ` がタスク終了後、ファイルのダウンロードで固まる

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
