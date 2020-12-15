wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O /tmp/packages-microsoft-prod.deb
dpkg -i /tmp/packages-microsoft-prod.deb

apt-get update
apt-get install -y apt-transport-https dotnet-sdk-3.1

apt-get install -y gnupg ca-certificates
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
echo "deb https://download.mono-project.com/repo/ubuntu stable-focal main" | tee /etc/apt/sources.list.d/mono-official-stable.list
apt-get update

mv /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.props /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.props
mv /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFx.targets /usr/share/dotnet/sdk/3.1.404/Sdks/Microsoft.NET.Sdk.WindowsDesktop/targets/Microsoft.WinFX.targets

apt-get install -y mono-devel python3 python3-pip
pip3 install -r requirements.txt
