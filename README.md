# PySpark_Local

Simple Repo for running pyspark locally using VS Code, Juypter Notebooks, Jetbrains or any other local IDE

## Installing PySpark locally with Notebooks

As we have seen over the past few weeks, there is an issue on google colab due to Java versions. Its not entirely fair to point the blame exclusivly at Java, as the incompatability comes from Java - Python V3.13 and PySpark 4.0.1.

I've discovered that it works best if we use Java v11, Python 3.11 and we can continue to use PySpark 3.5.1. I orginally treid to avoid introducing virtual envionrments as it introduces unnesseucary complexity; however this is the real world of software engineering. The method i prefer to use when managing complex dependancy relationships is UV. To setup Pyspark locally and ensure the depndancies are managed we can go through the following process

### Installing UV

You will first need to install UV. This is fundamentally a package manager like PIP (python) however has the added benefit of allowing us to manage virtural enviornments and python versions as we would any other package.

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

You can find more documentaiton on getting started with UV (and installation) [here](https://docs.astral.sh/uv/getting-started/installation/#__tabbed_1_1)

There is more information and some useful tutorials on what UV is and some common cmds when using it here. Some of them are decent but i've also attached specfic notes on UV in general. Only review these as required, they are not that important but useful for getting and understanidng of the tool and any issues that might arrise.

- [UV Docs](https://docs.astral.sh/uv/)
-

### Setting up venv

With UV installed, we can proceed to setting up the virtual envionment and initilising spark locally (this should work for VS code, juypter notebooks and any other local development envionrmnet). We can do this in colab also if we want although its not really nessecary.

1. Install UV (we should have this done already. we can check by running the cmd uv --version)
2. Clone UV repo
3. Setup UV virtual envionment (cmd: uv venv). This will create a python 3.11 venv
4. activate the venv (cmd: .venv\Scripts\activate). This will activate the venv and allow us to install the dependancies.
5. run UV sync to pull all the required dependancies (cmd: uv sync). This will install all the depndancies to the venv

NOTE: i've placed the package files within blackboard also, so this essentally replaces step 2 if you dont want to clone the github repo i created. Lastly, when we are finished working in our venv, you can deactivate it by typing the cmd 'deactivate'.

Once we have that done, we can then import pyspark and initlise our spark session

```Python

import sys, os, findspark
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

findspark.init()

spark = SparkSession.builder.appName("SparkTest").getOrCreate()

print(f"findspark version: {findspark.__version__}")
print(f"pyspark version: {spark.version}")

```

This should output the following:

findspark version: 2.0.1
pyspark version: 3.5.7

## Common errors

This is the most common errors unique to windows users. Its a classic pyspark incompatability error related to pyspark version 4.0.1 and (typically) python version 3.13. Although, not specfically limited to 3.13. To rectify this error, in the venv we attempt to pin python version to 3.11 and install pyspark version == 3.5.1

```Python
Py4JJavaError                             Traceback (most recent call last)
Cell In[9], line 1
----> 1 df.collect()

File c:\Users\Tony\OneDrive - Ulster University\Teaching\COM739\2025-26\.venv\Lib\site-packages\pyspark\sql\classic\dataframe.py:443, in DataFrame.collect(self)
    441 def collect(self) -> List[Row]:
    442     with SCCallSiteSync(self._sc):
--> 443         sock_info = self._jdf.collectToPython()
    444     return list(_load_from_socket(sock_info, BatchedSerializer(CPickleSerializer())))

File c:\Users\Tony\OneDrive - Ulster University\Teaching\COM739\2025-26\.venv\Lib\site-packages\py4j\java_gateway.py:1362, in JavaMember.__call__(self, *args)
   1356 command = proto.CALL_COMMAND_NAME +\
   1357     self.command_header +\
   1358     args_command +\
   1359     proto.END_COMMAND_PART
   1361 answer = self.gateway_client.send_command(command)
-> 1362 return_value = get_return_value(
   1363     answer, self.gateway_client, self.target_id, self.name)
   1365 for temp_arg in temp_args:
   1366     if hasattr(temp_arg, "_detach"):

File c:\Users\Tony\OneDrive - Ulster University\Teaching\COM739\2025-26\.venv\Lib\site-packages\pyspark\errors\exceptions\captured.py:282, in capture_sql_exception.<locals>.deco(*a, **kw)
    279 from py4j.protocol import Py4JJavaError
    281 try:
...
Caused by: java.io.EOFException
	at java.base/java.io.DataInputStream.readInt(DataInputStream.java:386)
	at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:933)
	... 26 more
```
