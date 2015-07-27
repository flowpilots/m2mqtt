
REM MSBuild.exe M2Mqtt-FlowPilots.sln /p:Configuration=Release

IF NOT EXIST ".\Build\Packages" MKDIR ".\Build\Packages"

.\Tools\NuGet\NuGet.exe pack .\nuspec\M2Mqtt-FlowPilots.nuspec -OutputDirectory ".\Build\Packages"
