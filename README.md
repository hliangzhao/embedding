# Dependent Function Embedding
``embedding`` is a python package for solving the dependent function embedding problems. 

This is the repos for our paper accepted by IEEE INFOCOM 2021 **Placement is not Enough: 
Embedding with Proactive Stream Mapping on the Heterogenous Edge** (add a link). If you are using this 
code, please cite this paper. Currently, you can cite the arXiv version:
```
@INPROCEEDINGS{Zhao2105:Placement,
AUTHOR="Hailiang Zhao and Shuiguang Deng and Zijie Liu and Zhengzhe Xiang and
Jianwei Yin",
TITLE="Placement is not Enough: Embedding with Proactive Stream Mapping on the
Heterogenous Edge",
BOOKTITLE="IEEE INFOCOM 2021 - IEEE Conference on Computer Communications (INFOCOM
2021)",
ADDRESS="Vancouver, Canada",
DAYS=10,
MONTH=may,
YEAR=2021,
KEYWORDS="edge computing; dependent function embedding; directed acyclic graph;
function placement"
}

```

Dependent function embedding is the combination of **function placement** and **stream mapping** 
on the heterogenous edge. Function placement studies how the dependent functions in an app 
(abstracted as a DAG) are placed on edge servers to minimize the makespan. Stream mapping 
studies how the data stream transferred between each function pair are mapped to different 
links between edge servers.

``embedding`` implements the algorithm DPE (Dynamic Programming-based Embedding) proposed in this paper
and two contrastive algorithms, FixDoc (https://dl.acm.org/doi/10.1145/3326285.3329055) and 
HEFT (https://ieeexplore.ieee.org/document/993206).

## Installation
You can run this code directly by downloading this repos into your desktop. 

To install ``embedding`` by source code, download this repository and sequentially run following 
commands in your terminal/command line:
```commandline
python setup.py build
python setup.py install --record files.txt
```
If you want to uninstall this package, please run the following command in the same directory. 
For linux/macOS:
```commandline
xargs rm -rf < files.txt
```
For windows powershell:
```commandline
Get-Content files.txt | ForEach-Object {Remove-Item $_ -Recurse -Force}
```
You can permanently uninstall this package by further deleting the directory 
``../lib/python3.7/site-packages/embedding-0.1.egg/``.


## A simple example
Our implementation is based on the Alibaba cluster trace dataset (https://github.com/alibaba/clusterdata), 
Please use the v2018 and download the file *batch_task* through 
http://clusterdata2018pubcn.oss-cn-beijing.aliyuncs.com/batch_task.tar.gz. **The package does not include 
 this file because it's too large.** In default file path settings, you may put the 
 uncompressed file into the directory ``embedding/dataset/``.

The example consists of three steps. Firstly, sampling DAGs from the batch_task.csv file and 
get the topological order for each DAG. 
```python
from embedding.dataset_processing import sample_DAG, get_topological_order

sample_DAG(batch_task-file-path)
get_topological_order()
```
Secondly, generate the edge computing scenario, i.e., a connected graph of edge servers,
including the connectivity, processing power of each server, and bandwidth of each physical link.
```python
from embedding.scenario import *

G, bw, pp = generate_scenario()
print_scenario(G, bw, pp)
simple_paths = get_simple_paths(G)
print_simple_paths(simple_paths)
reciprocals_list, proportions_list = get_ratio(simple_paths, bw)
pp_required, data_stream = set_funcs()
```
Thirdly, run the three algorithms and compare the results. The code below prints the 2011th 
DAG's scheduling results.
```python
from embedding.algos.dpe import DPE
from embedding.algos.fixdoc import FixDoc
from embedding.algos.interpretate_result import *
from embedding.parameters import *

dpe = DPE(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)
T_optimal_all_dpe, DAGs_deploy_dpe, process_sequence_all_dpe = dpe.get_response_time(sorted_DAG_path=SORTED_DAG_PATH)
print_scheduling_results(T_optimal_all_dpe, DAGs_deploy_dpe, process_sequence_all_dpe, 2010)

fixdoc = FixDoc(G, bw, pp, simple_paths, reciprocals_list, proportions_list, pp_required, data_stream)
T_optimal_all_fixdoc, DAGs_deploy_fixdoc, process_sequence_all_fixdoc = fixdoc.get_response_time(sorted_DAG_path=SORTED_DAG_PATH)
print_scheduling_results(T_optimal_all_fixdoc, DAGs_deploy_fixdoc, process_sequence_all_fixdoc, 2010)
```

You can directly run ``example/example.py`` to obtain the results. Our experiments show that 
DPE outperforms these algorithms.
<div align=center>
    <img src="./img/comparison.png" width="400"/>
</div>


## Final Notes
For more details of our model and algorithm, please read our paper directly. You 
can find it on http://hliangzhao.me/papers/embedding.pdf.