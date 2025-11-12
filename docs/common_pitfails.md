# Installation and activations errors

### Error 1: java.io.EOFException

This is the most common errors unique to windows users. Its a classic PySpark incompatibility error related to PySpark version 4.0.1 and (typically) python version 3.13. Although, not specifically limited to 3.13. To rectify this error, in the venv we attempt to pin python version to 3.11 and install PySpark version == 3.5.1. Following successful deployment of the venv we should see this error resolve.

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

### Error 2: Cannot access file (used by another process)

This error prevents us from creating the required UV venv and therefore deploying PySpark. Typically this is caused by something interfering with UV's hard-link deployment methodology. Anyone who has previously used game modding might be aware of this, its basically a method where unlike pip (where each venv gets its own copy of a dependency), UV maintains a central cache repo of dependencies and creates references between that repo and the associated venv which require it. Therefore resulting in much faster deployment and reduce disk requirements.

```bash
The process cannot access the file because it is being used by another process. (os error 32)
```

Or might look something like this...

```Python
error: Failed to install: debugpy-1.8.17-cp311-cp311-win_amd64.whl (debugpy==1.8.17)
  Caused by: failed to copy file from C:\Users\Tony\AppData\Local\uv\cache\archive-v0\Aop6_BH9tQl2byve7oOYJ\debugpy\adapter\clients.py to C:\Users\Tony\OneDrive - Ulster University\Teaching\COM739\PySpark_Local\.venv\Lib\site-packages\debugpy\adapter\clients.py: The process cannot access the file because it is being used by another process. (os error 32)
```

This is usually a OneDrive error. OneDrive is either updating the directory or indexing. Normally OneDrive can cause a lot of similar headaches therefore its best to work away from OneDrive on a directory that is not being synced. If thats not possible, the close down OneDrive - give it a minute and try again. Alternatively, the error could be caused by the dependency `debugpy` in this case being used by another instance of the IDE. So close down all IDE instances and refresh/restart the kernel. We can also kill any python processes that are using the venv by using this cmd

```bash
Get-Process python* | Stop-Process -Force # Check for running Python processes
Get-Process | Where-Object {$_.Path -like "*PySpark_Local*"} | Stop-Process -Force # Or specifically look for anything using your venv
```

### Error 2: ExecutionPolicy, cannot activate virtual environment

You machine execution policy can prevent the execution of various scripts, including ones you create yourself. While this is typically a windows/powershell related error its a typical reason that prevents some virtual environments from being activated. A `remoteSigned` policy allows us to run locally created or signed scripts from trusted publishers. If we limit the scope to `process` then it only applies to the current session. Alternatively we can apply to the current user to keep the settings persistent.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

We can check the current execution policies applied by running the cmd:

```powershell
Get-ExecutionPolicy -List
```

The output should be something like

```bash
Scope          ExecutionPolicy
-----          ---------------
MachinePolicy  Undefined
UserPolicy     Undefined
Process        RemoteSigned    ← Your current session only
CurrentUser    Undefined       ← Your user account
LocalMachine   Undefined       ← All users
```
