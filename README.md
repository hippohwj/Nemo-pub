# Nemo

Nemo: Efficient Caching with Index Pushdown for Disaggregated OLTP Databases

## System Requirements
- OS: Ubuntu 20.04 LTS or higher
- Toolchain: CMake ≥ 3.10, GCC/G++ ≥ 9, Python 3.8+
- Azure / Cosmos DB (Table API):
  - C++: 
    - `azure-storage-cpp` 7.5.0#6
    - azure-storage-blobs-cpp: 12.8.0
    - azure-storage-common-cpp: 12.3.3
  - Python helpers: `azure-storage-blob` ≥ 12.x and `azure-data-tables` ≥ 12.x (PyPI)
- Other dependencies: Boost, jemalloc, OpenSSL, etc.


## Install Dependencies

Install Azure Dependencies
```
cd tools
./install_azure.sh
```

## Configurations
Nemo's data store can be any remote storage services which can satisfy the APIs defined in our paper. 
Connecting urls should be configured to guide Nemo to correctly access the storage service. We use Azure CosmosDB by default. If you want to extend to other storage services, please change the configure file "configs/ifconfig\_azure.txt" to your own storage service and implement interfaces in IDataStore.h accordingly.

## Compile and Run
compile data loading exec
```
./compile_dataload.sh
```

compile experiment exec
```
./compile_exp_release.sh
```

expected output
```
-- Generating benchmark tests: 
-- [benchmark test] executable: bench_ycsb
-- Generating unit tests: 
-- [unit test] executable: AllUnitTests
-- [unit test] executable: CCTest
-- [unit test] executable: IndexObjBtreeTest
-- Generating experiments: 
-- [experiment] executable: ExpYCSB
-- Configuring done
-- Generating done
-- Build files have been written to: /users/username/Nemo/
```

## Load Data

After compiling data loading exec, we can generate and load data. Here is an example script to load 10G data. Parameters within cfg file can be changed to load different sizes of data.

```bash
cd build
./LoadYCSB configs/load_data_10g.cfg
```

## End-to-end Experiments
We support multiple types of end-to-end experiments for different scenarios (i.e., positive search, negative search, and phantom protection workloads). 

All scripts for experiments are in this directory: `experiments/`. Please change host IPs and storage connection keys within the scripts to run experiments in your environments. 

Here are example commands to run each type of end-to-end experiments:

### 1. Positive Search

Run an read-only experiment while varying the buffer size
```
cd experiments/
python3 ExpPointTupleVaryBuf.py > logs/ExpPointTupleVaryBuf.log
```

Run an read-write experiment while varying the buffer size
```
cd experiments/
python3 ExpPointTupleVaryBufRW.py > logs/ExpPointTupleVaryBufRW.log
```

Run an read-only experiment while varying skewness
```
cd experiments/
python3 ExpPointTupleUniHotspot.py > logs/ExpPointTupleUniHotspot.log
```

Run an read-write experiment while varying skewness
```
cd experiments/
python3 ExpPointTupleUniHotspotRW.py > logs/ExpPointTupleUniHotspotRW.log
```

### 2. Negative Search

#### (1) Negative Point Search

Run negative point search experiment while varying skewness
```
cd experiements/negative_search
python3 PointVarySkewnessD2g.py > logs/PointVarySkewnessD2g.log
```
Run negative point search experiment while varying negative ratio
```
cd experiements/negative_search
python3 PointVaryDomain03BufferSkewTuple.py > logs/PointVaryDomain03BufferSkewTuple.log
```

#### (2) Range Search

Run (partial negative) range search experiment while varying skewness
```
cd experiements/negative_search
python3 RangeVarySkewnessD2gScatter.py > logs/RangeVarySkewnessD2gScatter.log
```

Run (partial negative) range search experiment while varying range size
```
cd experiements/negative_search
python3 RangeVaryScanLenD2gScatter.py > logs/RangeVaryScanLenD2gScatter.log
```

### 3. Phantom Protection

Run phantom protection experiments (mix workloads of range and insert queries) while varying domain size of the key
```
cd experiements/phantom_protection
python3 PPVaryDomainFixLockTblSize.py > logs/PPVaryDomainFixLockTblSize.log
```

### 4. Ablation Study

#### 4.1 YCSB on the Simulated Store

10.24M rows x 1KB (~10GB), zipf 0.9, 16 workers, buffer ratio {1,5,10,25,50,75,100}%.

Data loading
```
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DREMOTE_STORAGE=SIMULATED \
      -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake ../
make -j ExpYCSB LoadYCSB SimStoreServer SimNetBench SimStoreTest

./LoadYCSB ../configs/load_data_sim_10g.cfg
./LoadYCSB ../configs/load_pages_sim_10g.cfg
./SimStoreTest
```

Run experiment
```
cd experiments/
NEMO_SIMNET_ADDR=127.0.0.1:34467 python3 ExpPointVaryBufForALLSim.py
```

#### 4.2 YCSB on Cosmos DB (100GB)

1e8 rows x 1008B (~100GB), record dataset `obj-100g-0` and page dataset `page250-100g-0`, 16K RU/s.

Data loading
```
bash experiments/dump_ycsb_ref.sh
python3 experiments/synth_ycsb.py --selftest \
        experiments/ref_100g/ref_head.tsv \
        experiments/ref_100g/ref_mid.tsv \
        experiments/ref_100g/ref_tail.tsv
bash experiments/run_import.sh 40000 4
bash experiments/run_page_import.sh 40000 4
python3 experiments/verify_100g.py --conn-file $NEMO_AZURE_CONF \
        --ref experiments/ref_100g/ref_head.tsv \
        --manifest-glob "$NEMO_BASE/manifests/m_*.txt" --write-count
```

Run experiment
```
cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake ../
make -j ExpYCSB
cd ..

export NEMO_REPO=$PWD
bash experiments/p3_base16k.sh
bash experiments/p3_2tree_fixed.sh
bash experiments/p3_full16k.sh
```

#### 4.3 TPC-C on Cosmos DB (34 Warehouses)

34 warehouses (~4.93GB) over the nine standard TPC-C tables, standard transaction mix (45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel), 16 workers, buffer ratio {1,10,25,50,75}%.

Data loading
```
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DREMOTE_STORAGE=SIMULATED \
      -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake ../
make -j LoadTPCC

./LoadTPCC ../configs/dump_tpcc_sim_34w.cfg
./LoadTPCC ../configs/dump_pages_tpcc_sim_34w.cfg

cd ..
python3 experiments/shard_dump.py --by-table /tmp/nemo-dumps/o34w <shards-root>
python3 experiments/tpcc_ru_plan.py --shards-root <shards-root> --budget 40000 > plan.tsv
bash experiments/run_tpcc_import.sh plan.tsv <shards-root> <tag>
python3 experiments/verify_tpcc.py --conn-file $NEMO_AZURE_CONF \
        --shards-root <shards-root> --manifest-dir <manifests>
```

Run experiment
```
cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake ../
make -j ExpTPCC
cd ..

export NEMO_REPO=$PWD
bash experiments/tpcc_cosmos_grid.sh 0.01,0.10,0.25,0.50,0.75 nemo,btree-ri
```

#### 4.4 Buffer Replacement Microbenchmark

10.24M rows at 16/32/64/128B and 1.024M rows at 1KB, 10% buffer, zipf 0.9, 16 workers.

Data loading
```
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DREMOTE_STORAGE=SIMULATED \
      -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake ../
make -j ExpYCSB LoadYCSB SimStoreServer

./LoadYCSB ../configs/load_data_sim_1g.cfg
./LoadYCSB ../configs/load_data_sim_10g_r16.cfg
./LoadYCSB ../configs/load_data_sim_10g_r32.cfg
./LoadYCSB ../configs/load_data_sim_10g_r64.cfg
./LoadYCSB ../configs/load_data_sim_10g_r128.cfg
```

Run experiment
```
cd experiments/
NEMO_POLICIES=nemo,clock,lru NEMO_RECSZ=128 python3 ExpEvictPolicySmoke.py
```

## Code Structure
```
├── README.md                  # Project README file
├── benchmark                  # Code for classic benchmarks (such as YCSB), including data loading, query generation, and benchmark configurations
│   ├── ycsb                       # YCSB workload
│   ├── tpcc                       # TPC-C workload
├── configs                    # Configuration files for setting parameters (mainly used for data loading)
├── debug                      # GDB debug tools
├── output                     # Output results of all experiments, plus the archived findings and measurement data behind the ablation study
├── experiments                # End-to-end experiments for typical scenarios, plus the drivers, data import and reporting scripts of the ablation study
│   ├── ExpYCSB.cpp                # Main entrance of experiments
│   ├── LoadYCSB.cpp               # Main entrance of data loading
│   ├── ExpTPCC.cpp                # Main entrance of TPC-C experiments
│   ├── LoadTPCC.cpp               # Main entrance of TPC-C data loading
│   ├── SimStoreServer.cpp         # Main entrance of the simulated store server (networked mode)
│   ├── negative_search            # python scripts to run negative search experiments
│   ├── phantom_protection         # python scripts to run phantom protection experiments  
├── src                        # Core logic for the OLTP testbed 
│   ├── common                 # Common utilities such as global variables, basic data structures, basic types, etc.
│   ├── db                     # Core logic for OLTP-related features, such as data storage, transaction management, and remote storage communication
│   │   ├── access                     # Index interfaces and implementations
│   │   │   ├── IndexBTreeObject.cpp                   # In-memory index for tree-structured record buffer (phantom locks are acquired here). 
│   │   │   ├── IndexBTreePage.cpp                     # Page-based B+tree
│   │   ├── buffer                     # Buffer management (i.e., strategies for cache admission and eviction). Both record-based and page-based managements are implemented
│   │   ├── ARDB.cpp                   # DB engine 
│   │   ├── CCManager.cpp              # Responsible for concurrency control (2PL by default). Phantom locks are also implemented here.
│   │   ├── ITxn.cpp                   # Core logic for transaction execution and atomic commit
│   │   │   ...
│   ├── local                  # Compute-layer integration and basic units
│   │   ├── ITable.cpp                 # The entrance for DB-level CRUD. It coordinates core components to produce results
│   │   ├── ITuple.cpp                 # Record Object
│   │   ├── IPage.cpp                  # Page Object
│   ├── remote                 # Codes for accessing remote storage, such as writing logs to the log store, reading from / writing updates to the data store (e.g., CosmosDB). The backend is selected at compile time by -DREMOTE_STORAGE
│   │   ├── ILogStore.cpp                  # Access remote log store
│   │   ├── IDataStore.cpp                 # Access remote data store
│   │   ├── SimNetClient.cpp               # Simulated store client
│   │   ├── SimNetServer.cpp               # Simulated store server
│   │   ...
├── tools                      # Scripts for installing dependencies
├── unit_tests                 # Some unit tests
├── compile_exp_release.sh     # Scripts for compiling experiment exec codes
├── compile_dataload.sh        # Scripts for compiling data loading exec codes
├── compile_exp_debug.sh       # Scripts for compiling experiment exec codes (debug version)
```
