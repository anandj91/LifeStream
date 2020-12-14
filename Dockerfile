FROM ubuntu:20.04

ADD ./LifeStream /root

RUN apt-get update
RUN apt-get install -y wget

RUN wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb

RUN apt-get update
RUN apt-get install -y apt-transport-https dotnet-sdk-3.1

RUN apt-get install -y gnupg ca-certificates
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN echo "deb https://download.mono-project.com/repo/ubuntu stable-focal main" | tee /etc/apt/sources.list.d/mono-official-stable.list
RUN apt-get update

RUN mv /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.props /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.props
RUN mv /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.targets /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.targets

RUN apt-get install -y mono-devel python3 python3-pip
RUN pip3 install -r /root/requirements.txt
