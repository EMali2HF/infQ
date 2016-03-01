# infQ
======
A queue whose capacity isn't limited by memory can be integrated with redis.
*infQ*是一个容量不受限于内存的队列。在保证性能不变的情况下，将中间部分数据落地磁盘以节省内存空间。


*infQ*用于替换redis的list类型，避免在队列场景下，由于消费能力不够导致的拥堵、内存耗尽。设计文档见[infQ——不受限于内存的队列](http://blog.csdn.net/chosen0ne/article/details/50766895)

##Features
* 支持大部分list操作：push，pop，len，top，at等。
* 支持序列化及反序列化。
* 支持通过磁盘缓冲数据，以节省内存。

##Install
轻量级，没有外部依赖。在项目路径下，执行下列命令，进行编译：
> make

##Example
示例代码：
>   #include <stdio.h>
>   #include "infq.h"
>   
>   int main() {
>       int     data, *val, size;
>       infq_t  *q;
>       
>       q = infq_init("./data", "infq_test");
>       if (q == NULL) {
>           printf("failed to init infq\n");
>           return -1;
>       }
>       
>       // try to push
>       data = 100;
>       if (infq_push(q, &data, sizeof(data)) == INFQ_ERR) {
>           printf("failed to push data to infQ\n");
>           return -1;
>       }
>       
>       // try to pop
>       if (infq_pop_zero_cp(q, &val, &size) == INFQ_ERR) {
>           printf("failed to pop data from infQ\n");
>           return -1;
>       }
>       printf("pop data from infQ, data=%d\n", *val);
>       
>       return 0;
>   }


具体的运行代码参见*src/main.c*，序列化相关参见*src/persistent_dump_test.c*和*src/persistent_load_test.c*
