
主要流程：
	原csv文件---->转边集脚本处理-->边集文件-->分区算法处理-->分区后的边集文件-->转csv文件处理-->分区后的新csv文件

下面将以主要流程中的处理顺序说明：

（1）原csv文件
	将全部31个csv文件放于一个文件夹下，文件夹命名为import（或者自己修改代码中的文件夹名称）。

（2）转边集脚本处理
	转边集的脚本针对分区算法程序输入文件格式的不同一共有三个，分别为
	NE：csv2edges_NE.py 分隔符为空格
	Sheep：csv2net_sheep.py 分隔符为空格
	HDRF：csv2txt_HDRF.py  分隔符为\t
	以上三个脚本会读取所有点集csv文件并为每个点赋予一个新id，同时将新id与原id的对应关系存于前缀为vexKey的txt文件中，把每种label点的新id取值范围存于前缀为vexBelong的txt文件中。
	接着会读取所有的边集csv文件，把每条边的起点与终点新id存储下来，最终保存到名为result的边集文件中，文件类型由所使用的脚本决定，分为edges、net和txt。例如，使用NE脚本会得到result_NE.edges文件。

（3）边集文件
	由（2）得到的一个边集文件，是分区算法程序的输入。

（4）分区算法处理
	所有分区算法程序均由paper作者编写提供，我只稍微修改了HDRF算法程序(Edge的compareTo直接返回-1)，删除了去重复边和增加了输出方便查看程序运行进度。
	NE:
	     源代码地址为https://github.com/masatoshihanai/DistributedNE
	     使用步骤：
		1、进入到/data1/hzy/neo4j/partition_code/code/NE/build/目录下（fix:修改项目CMakeLists.txt为DistributedNE-CMakeLists.txt）
		2、运行命令mpirun -n <进程数> ./DistributedNE <边集文件地址> <分区数>，ps：进程数与分区数相等
		3、运行完成后会生成一个记录有分区信息的边集文件，可能存放在程序目录内或者边集文件地址下
	    其余程序使用详情请看源代码github页面的说明
		

	Sheep：
	    源代码地址https://github.com/dmargo/sheep
	    使用步骤：
		1、进入到/data1/hzy/neo4j/partition_code/code/sheep/目录下(fix:lib/sequence.h添加头文件#include <cstring>)
		2、运行命令./scripts/dist-partition.sh [options... -o 分区后的文件输出地址（文件前缀）] 边集文件地址 分区数
		3、该程序分区后会输出多个文件，每个分区都是一个单独的边集文件，形如data0000、data0001、data0002......
	    其余程序使用详情请看源代码github页面的说明
	    该程序分区完成后会单独输出每个分区的文件，需要使用脚本sheep_merge.py，将所有分区文件合并成一个分区文件

	HDRF：
	    源代码地址https://github.com/fabiopetroni/VGP
	    使用步骤：
		1、进入到/data1/hzy/neo4j/partition_code/code/VGP/目录下(fix:直接使用code目录下VGP.jar替换VGP/dit目录底下VGP.jar，Edge的compareTo直接返回-1修复不同label边去重问题)
		2、运行命令java -Xms128M -Xmx128M -jar dist/VGP.jar 边集文件地址 分区数 -algorithm hdrf -lambda 3 -threads 1 -output 分区后的文件输出地址
		3、注意一定要通过-Xms和-Xmx选项去修改jvm运行时内存大小，否则读取边的速度在占用内存达到jvm限制时会变得非常慢，-Xms和-Xmx设置的内存大小最好大一点，我处理
		     26G大小的文件时设置了200G内存
		4、运行完成后会生成三个文件，其中一个是记录有分区信息的边集文件
	    其余程序使用详情请看源代码github页面的说明
	```
	// VGP/core/Edge.java
	@Override
    public int compareTo(Object obj) {
        if (obj == null) {
            System.out.println("ERROR: Edge.compareTo -> obj == null");
            System.exit(-1);
        }
        if (getClass() != obj.getClass()) {
            System.out.println("ERROR: Edge.compareTo -> getClass() != obj.getClass()");
            System.exit(-1);
        }
        final Edge other = (Edge) obj;
        // return this.toString().compareTo(obj.toString()); //lexicographic order
        return -1;
    }
	 IDEA 实现「Build 带 lib 依赖的 VGP.jar」（核心步骤）
	你需要的是生成可执行的 VGP.jar + 自动复制依赖到 lib 文件夹（对应之前的 Build 说明），IDEA 需先配置「Artifact」，再执行 Build：
	步骤 1：确认主类（包含 main 方法）
	先确保项目中有可运行的主类，且能正常运行：
	打开包含 main 方法的类 → 右键 → Run 'XXX.main ()'（运行一次，生成运行配置）。
	步骤 2：配置 Artifact（关键，对应「打包 JAR + 依赖」）
	打开项目结构：顶部菜单栏 → File → Project Structure（快捷键：Ctrl + Alt + Shift + S/Cmd + ;）；
	选择左侧「Artifacts」→ 点击右上角「+」→ 选择「JAR」→ 「From modules with dependencies...」；
	在弹出的窗口中配置：
	Main Class：点击下拉框，选择你的主类（比如 com.xxx.Main）；
	JAR files from libraries：选择「Copy to the output directory and link via manifest」（核心！这会把依赖复制到 lib 文件夹，并在 MANIFEST.MF 中添加 Class-Path）；
	Output directory：选择项目目录下的 dist 文件夹（比如 D:\your-project\dist）→ 点击「OK」；
	回到 Artifacts 页面，确认：
	右侧「Output Layout」中能看到你的主类和依赖的 JAR；
	下方「Manifest file」会自动关联，且「Main Class」已填充。
	步骤 3：执行 Build（生成 VGP.jar + lib 文件夹）
	顶部菜单栏 → Build → Build Artifacts；
	在弹出的列表中，找到你配置的 Artifact（比如 VGP:jar）→ 选择「Build」；
	等待构建完成后，打开你指定的 dist 文件夹，会看到：
	plaintext
	dist/
	├── VGP.jar       # 可执行主JAR（名字可在Artifact配置中修改）
	└── lib/          # IDEA自动复制的依赖JAR（和你要的Build说明完全匹配）
		├── xxx.jar
		├── yyy.jar
		└── ...
	步骤 4：验证运行（和说明一致）
	进入 dist 目录，执行命令：
	bash
	运行
	java -jar VGP.jar
	如果运行报错，大概率是依赖缺失，回到 Artifacts 配置页面，检查「Output Layout」是否包含所有依赖的 JAR。
	```

（5）分区后的边集文件
	分区完成后都会有一个记录有分区信息的边集文件，其中Sheep需要调用脚本sheep_merge.py把所有分区文件合并成一个分区文件方便后续使用

（6）转csv文件处理
	针对不同分区算法程序生成的边集文件，需使用对应的转csv脚本
	NE：edges2csv_NE_process_V3.py
	Sheep：edges2csv_sheep_process_V3.py
	HDRF：edges2csv_HDRF_process_V3.py

	运行这些脚本所需的输入文件一共有三个，（2）中生成的点信息文件vexKey和vexBelong，（4）中生成的分区后的边集文件
	运行前要修改脚本代码里的各个输入文件地址和文件输出地址

（7）分区后的新csv文件
	（6）中程序运行完后会生成各个分区的csv文件，分别放在对应的以数字命名的文件夹下。例如针对4个分区的情况，（6）中会生成4个文件夹到文件输出路径下，分别为0，1，2，3，代表4个分区
	  每个文件夹下都是31个csv文件，其中存放着该分区下的所有点和边的信息。

** 以上代码均建立在csv文件是以“|”分隔的
