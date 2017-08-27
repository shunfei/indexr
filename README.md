# IndexR

![IndexR Logo](images/indexr-logo-150x150.png)

**IndexR** is a super fast columnar data formate on HDFS, which focus on fast analytic, both for massive static(historical) data and rapidly ingesting realtime data. IndexR is designed for OLAP. IndexR is greatly suitable for building data warehouse based on Hadoop ecosystem.

* Super fast, 2~4x read speed of Parquet.
* 3 levels indices supported. Say goodbye to full scan.
* Support realtime ingestion. No more wait, analyse anything right after they happen.
* Hardware efficiency, anyone can use.
* Features like realtime and offline pre-aggregation, online schema update, 100% accurate, etc.
* Deep integration with Hadoop ecosystem. Adapted with popular query engines like Apache Drill, Apache Hive, etc.

#### Getting started

* Installation
  * First [Compile from source](https://github.com/shunfei/indexr/wiki/Compilation) or download a pre-compiled package directly from [release page](https://github.com/shunfei/indexr/releases).
  * Then [Set up a cluster](https://github.com/shunfei/indexr/wiki/Deployment).
* User manual - Check [here](https://github.com/shunfei/indexr/wiki/User-Guide).
* Any problems? - Found an [issue](https://github.com/shunfei/indexr/issues).

#### Documentation

[https://github.com/shunfei/indexr/wiki](https://github.com/shunfei/indexr/wiki)

#### Useful Links

* [IndexR 技术白皮书](https://github.com/shunfei/sfmind/blob/master/indexr_white_paper/indexr_white_paper.md)
* [IndexR introduction](https://github.com/shunfei/sfmind/blob/master/indexr.about.en.md)

Please feel free to file any issues.

## Contact

* Email: 
	* <indexree@vectorlinex.com>
	* <519178957@qq.com>
* IRC: #indexr
* QQ Group: 606666586 (IndexR讨论组)

## License

    Copyright 2016 Sunteng Tech.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
