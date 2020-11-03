# 作业6

## 作业要求

给定一个包含上千万用户的社交网络，尝试编写MapReduce程序为所有用户对中找出“共同好友”。

即令U为包含所有用户的一个集合：{U1, U2, ….Un}，编写MapReduce程序为每个(Ui, Uj)对（i≠j）找出共同好友。假设输入记录格式如下：

```
<person><,><friend1><friend2>…<friendn>

这里<friend1><friend2>…<friendn>是<person>的好友，均用唯一的用户ID标识。

输出格式为：<([><personi,personj><]><,><[><friendx><,><friendy><,>….<friendz><])>
```
编写MapReduce程序可以尝试两种方案（1）使用基本数据类型；（2）使用自定义数据类型。

例如输入实例：

100, 200	300	400	500	600

200, 100	300	400

输出为：

([100, 200], [300, 400])

## 设计思路

## 实验结果

## 思考

### 关于maven的各种玄学问题

couldn't transfer metadata https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-archetype-plugin/maven-metadata.xml.

这是我在从archetype创建项目时出现的报错。类似这样的玄学报错在作业五就遇到了很多很多很多，多是以couldn't transfer metadata/descriptor开头，搞得我一度很心累。我这次所幸不去找各种奇葩而难行的解决方案了，直接去maven的中央仓库手动下载了archetype的jar包，放到本地仓库。结果开始了下载过程，却直接报错找不到对应的archetype了。。。于是我决定弃用archetype。