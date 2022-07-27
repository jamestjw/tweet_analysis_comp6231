# Run instructions
Setup

Download the graphframes jar from [here](https://spark-packages.org/package/graphframes/graphframes) and place it in the Pyspark installation directory (e.g. `/usr/local/lib/python3.7/dist-packages/pyspark/jars/graphframes-0.8.2-spark3.1-s_2.12.jar`).

Have to use Java 8 for Spark, your Java8 Home directory might be elsewhere
```bash
export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home/"
```

Install python dependencies
```bash
pip install -r requirements.txt
```

Install DBScan library
```bash
git clone https://github.com/SalilJain/pyspark_dbscan.git
```