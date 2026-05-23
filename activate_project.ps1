$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot"
$env:Path = $env:Path -replace "C:\\Program Files\\Common Files\\Oracle\\Java\\javapath;",""
$env:Path = $env:Path -replace "C:\\Program Files \(x86\)\\Common Files\\Oracle\\Java\\java8path;",""
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
$env:HADOOP_HOME = "C:\hadoop"
$env:Path += ";C:\hadoop\bin"
olist-pipeline\Scripts\activate
Write-Host "Environment ready! Java: $(java -version 2>&1 | Select-Object -First 1)"
