# New Scheduler Design Document

Asakusa on M3BPは分散処理のオーバーヘッドを省略してマルチスレッドで
DAGのバッチ処理を実行するコンパイラとその処理系である。これを使えば、
Spark や Hadoop ではユーザーの本番環境では規模が大きくて投資対効果が小
さかった分野でもバッチを並列処理できる。しかしながら、実際の環境では小〜
中規模の日次または月次処理が多数あり、既存のジョブ管理システムではこれ
をうまくスケジューリングすることができない [要出典] 。具体的には、以下
の要件がある。

* 各タスクのCPU, メモリなどのリソース使用をうまく分離すること
* 複数台のノードがあったときに、どのノードでも同様に透過的かつフェアに起動すること
* 失敗したときに再実行すること
* ノード故障に伴い縮退環境になった場合でも、全ての処理を破棄するのではなく順に処理すること
* 任意のタイミングでジョブを投入し、リソースが確保できた段階で処理すること
* ジョブはなるべく投入された順番に実行されること
* 待機中のジョブを閲覧、管理できること
* システム全体が理想的なリソース使用率 (80% ~ 95%) を維持すること
* 全てのジョブが最短で終了できるようにリソース管理、ジョブスケジューリングすること

#### 未実装および制限事項

* ジョブの kill: ニーズが分かっていない
* ジョブをまとめて投入 `schedule -file`: ニーズが分かっていない

#### ロードマップ

* ジョブ終了後もステータスを保存する
* 特殊なリソースの指定: [GPU (1.0~)](https://issues.apache.org/jira/browse/MESOS-4424) など
* ジョブ登録などに認証機構
* クライアント毎にIsolation
* Docker などコンテナ上での実行
* キューの永続化およびFO
* Shutdown hook ( `java.lang.Runtime.addShutdownHook(Thread)` ) でタスク終了を待ち、キュー内容を保存・閉塞して Graceful stop する
* Mesos への認証アクセス
* ジョブのリトライ
* ワークフローエンジンとの連携

以下、新しい分散スケジューラのアーキテクチャ、構成要素、開発部分などを定義する

## アーキテクチャおよび構成要素

複数ノードのリソース管理システムには
[Apache Mesos](http://mesos.apache.org) を採用する。Mesosが持っている
主な機能は

* 処理ノード (Agent) の死活監視、資源管理、デプロイ
* 実行中のジョブ (Mesos上では Task）の状態管理
* フレームワーク（Mesosを利用するアプリケーション…を管理するもの）への資源の割当て

である。これに加えて、Mesos上で動作させたいサーバープロセスを管理する
汎用フレームワーク [Marathon](https://mesosphere.github.io/marathon/)
を動作させる。オプションで [Chronos](https://mesos.github.io/chronos/)
を追加してもよい。これらのシステムは、
[ZooKeeper](http://zookeeper.apache.org) を使用して高可用モードで動作
させることができる (* MesosはZKなしでも動作するが、Marathonおよび
ChronosはZKが必要）。ZooKeeperを高可用構成で利用することで、ネットワー
ク分断が起きてもシステムは矛盾なく動作を継続することができる。

これに加えて、 Asakusa on M3BP のジョブを管理するフレームワークとし
て Retz を開発する。Retz が持つ機能は、

* Asakusa on M3BP ジョブの実行およびキューイング（オンメモリ）
* Asakusa on M3BP に必要なアプリケーションのデプロイ、管理

である。図は、高可用構成で構築した環境である。高可用性が不要でダウンタ
イムやバッチの再実行を許容できる環境では、ZooKeeperおよびMarathonを省
略することができる（単にZooKeeperを1台構成で起動してもよい）。

```
+----------------------+
| on-M3BP-Job <- Agent-+----ZK------------+
+------------------^---+    |             |
                   |    +---+----+   +----+-----+
                   +----| Master |---| Marathon |
                        +---^----+   +----+-----+
                            |             |
  Operator ------------>  Retz <----------+
                           ^
                           |
  User ---(throw a job)----+

--> manage
--- connected
```

RetzのプロセスをMarathon に監視させれば、ノードの故障時にも自動的に
Marathonが自動的にフェイルオーバーする。しかし、ジョブキューは永続化さ
れないので、フェイルオーバー時にはジョブの再投入が必要。

## Retz client

Retzクライアントは、 `retz-client` というパッケージをいくつかのLinuxディ
ストリビューションで配布する。必要であれば、
`retz-client-<version>-all.jar` というFatJarを作成、デプロイして利用す
ることもできる。

#### アプリケーションの登録、管理

```sh
$ retz-client load-app --name your-app-name \
   -P [hdfs|http|https]://path/to/assembly.tgz[,http://example.com/another-file] \
   -L [hdfs|http|https]://path/to/assembly.tgz[,http://example.com/another-file] \
   -F [hdfs|http|https]://path/to/assembly.tgz[,http://example.com/another-file] \
   -disk 2048
$ retz-client get-app -A your-app-name
$ retz-client unload-app --name your-app-name
$ retz-client list-app
```

ジョブを実行する前に、Asakusa on M3BP アプリケーションの実行プログラム
（デプロイメントアーカイブ）、入力データなどをMesosが利用および配置で
きるようになっていなければならない。これには `load-app` というコマンド
を利用する。

`load-app` はアプリケーション、ジョブの実行環境をRetzに登録する。拡張
子が圧縮ファイルのものである場合はMesos Agent がこれを展開する。 `hdfs://` 
をスキームとして利用すると、HDFS上のファイルも取得することができる。ただし、
`hadoop` コマンドが Mesos Agent および Executor から利用可能な状況に
なっていなければならない。

一度登録したアプリケーションのファイルを追加、削除するには一度登録解除 (
`unload-app` ) すること。 `unload-app` を行うと、そのアプリケーション
のために確保されていた Persistent Volume も削除予約される。登録されている
アプリケーションの一覧を見るには `list-app` を実行する。

内部的には、このタイミングでExecutorを起動、作成する。Executor idは
`your-app-name` と一致する。実際の `yaess-batch.sh` のコマンドは
`TaskInfo.Builder#setData(Bytes[])` で渡される。

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
  そこにファイルを展開する
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

#### ジョブの実行

ジョブの投入には `schedule` および `run` サブコマンドを利用する。

```sh
$ retz-client schedule OPTIONS
$ retz-client run OPTIONS
```

`schedule` はジョブをRetzのキューに登録する。 `run` はキューに登録後、
自分が登録したキューの終了まで待機する。終了後は結果をローカルに収集す
る。後述の `OPTIONS` にはジョブの内容を記述する。

投入されたジョブは、順番がくると Mesos slave (agent) が動作しているノー
ド上で実行される。 `-resultdir` または `-R` でディレクトリを指定してい
る場合、結果のファイルをいくつかローカルに取得する。 `-R -` とした場合は、
Executor と実行されたコマンドの標準出力を擬似的にローカルの標準出力に表示する。

#### サーバーの起動

```sh
# /opt/retz-server/bin/retz-server
```

設定ファイルに Mesos Master または ZooKeeper の正しいアドレスが書かれ
ていない場合はアクセスに失敗し、起動はできない。

サーバーは単体ではフォアグラウンドで動作する。バックグラウンド動作しロ
グを管理したい場合は [Supervisor](http://supervisord.org/) 等のツール
を使って適宜利用すること。ディストリビューションには supervisor の設定
サンプルが含まれている。

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
$ retz-client watch
$ retz-client config
```

`run` `schedule` のオプション `OPTIONS`


* `-cmd 'yaess/bin/yaess-batch.sh ...'` [must] 実際にリモートで起動されるYAESSのコマンド。
* `-A --application your-app-name ` [opt] 登録したアプリケーション名。
  これがない場合は、 `CommandExecutor` が使用される。
* `-E key=value` 環境変数を設定する。
* `-R <directory>` 標準出力などのファイルをローカルにコピーするディレクトリを指定する（ `run` のみ）
* `-N --name <jobname>` [opt default=retz-task-name-<app-name>]
* `-cpu <posint>-[<posint>]` [opt default=1-] ジョブを実行可能なCPU数の範囲。ジョ
  ブは通常、Mesosに与えられたCPU数または、ここで指定した最大値を実行時に設定する。
* `-mem <posint>-[<posint>]` [opt default=512-] in MB ジョブを実行可能なメモリ量の範囲。ジョ
  ブは通常、Mesosに与えられたRAM量または、ここで指定した最大値を実行時に利用する。
* `-gpu <posint>` [opt default=0] ジョブを実行可能なGPUの枚数。
  これを有効にするためには、サーバーの設定ファイルで `retz.gpu=true` を設定しておかなければならない。
* `-trustpvfiles` Persistent Volumes 上に展開されているファイルが完全であると信用する
* `-id <jobid>` ジョブの状態を表示する。`-R <dir>` を `run` と同様に指定することで、結果をローカルに取得できる
* `-file <filename>` [must if batch/home is omitted] **未実装**
* `-retry <posint>` [opt default=config] **未実装**

`-E key=value` の形で環境変数を指定するときに、Executor上での実行環境に合わせた値を利用できる。

* `$RETZ_CPU` Mesosによって割り当てられた実際のコア数
* `$RETZ_MEM` Mesosによって割り当てられた実際のメモリ量（MB）
* `$RETZ_PVNAME` Mesosによって割り当てられたPersistent Volumeの相対パス。 Persistent Volume がない場合は `.` になる。

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
  ることがわかっている。

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

#### 再実行

基本的にはMesosはジョブの再実行を行わず結果を通知するだけである。再実
行回数は設定ファイルに書く（Job毎に設定するのはTODO?）

Retz は失敗の通知をうけて何度か再実行を試みる（TODO: 再実行は実装されていない）。

結果は Retz に通知されるので、 M3BP のジョブを再スケジュールする [要検討]

#### 結果の収集

バッチ処理自体の結果はバッチ内で定義する（DirectIOなりでHDFSやRDBMSに
保存する）が、M3BPジョブの標準出力、標準エラー出力はジョブ終了後に、指
定のアドレスへアクセスする。アドレスはサーバーのログに表示される。

また、 `run` サブコマンドを利用した場合は `-R <dir>` を引数に指定する
ことで、Executorの標準出力および標準エラー出力をローカルに保存すること
ができる。

#### 耐障害性

Retz のサーバープロセスは耐障害性を考慮していないため、いちど終了し
た場合は再起動する必要がある。無停止で運用したい場合は Marathon のアプ
リケーションとして管理することを推奨する。 Marathon の 8080 番ポートに
Webブラウザでアクセスして Retz を設定すれば、あとはMarathonが自動的
に起動および設定してくれる。再起動後は大抵IPアドレスおよびポートが変わ
るので、Retz の HTTP に透過的にアクセスしたい場合は、
[Mesos-DNS または Mesos-lb](https://mesosphere.github.io/marathon/docs/service-discovery-load-balancing.html)
などのサービスディスカバリの仕組みを構築する必要がある
[参考](https://open.mesosphere.com/tutorials/service-discovery/)。

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

#### M3BP 向けの最適化 (Asakusa on M3BP で1台のNUMAマシン内を最適利用する)

一般的に 1つの Asakusa on M3BP ジョブをマシン内の全コアを利用して実行するより
も、適当な数のジョブにCPUを分割して割り当てた方が効率がよい（距離のあ
るNUMAノードにm3bp のスレッドを割り当ててしまうと、ノード間のバスに律
速されてパフォーマンスが低下する、と考えられている）

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

#### 監視

* HTTPに監視用のリソース `/ping`, `/status` がある。
* JMXのポートはまだ変更できない

`/ping` は、サーバーが起動していれば `OK` という2文字をBodyにして `200 OK` を応答する。
`/status` は、スケジューラおよびWebSocketサーバーの各種統計値を返す。

#### HTTP プロトコル


* `GET /job/:id` behind `get-job` subcommand

* `PUT /job` behind `schedule` `run` subcommand

Request: `{"command":"schedule", "job":Job, "doWatch":false}`
Response: `{"status":"ok", "job":Job1}` or `{"status":"queue full"}`

* `DELETE /job/:id` behind `kill` subcommand

**Note: not implemented yet**

Request: `{"command":"kill", "id":24}`
Response: `{"status":"ok"}` or `{"status":"not found"}`

* `GET /jobs` behind `list` command

Request: `{"command": "list"}`
Response: `{"status":"ok", "queue": [ Job0, Job1, .... ]}`

* `GET /apps` behind `list-app` command
* `PUT /app/:name` behind `load-app` command
* `DELETE /app/:name` behind `unload-app` command

* `watch` via WebSocket, behind `watch` command

`watch` はサーバーのURL `ws://hostname:9090/cui` とWebSocketを介して通信する。ここではそ
の通信仕様を定義する。

Request: `{"commnd":"watch"}`
Response: `{"status":"ok", "event":Event, "job":Job}}`

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
