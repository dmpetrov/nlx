#define MyAppName "DVC (Data Version Control)"
#define MyAppVersion ReadIni(SourcePath + "\config.ini", "Version", "version", "unknown")
#define MyAppPublisher "Dmitry Petrov"
#define MyAppURL "https://dvc.org"
#define MyAppDir SourcePath + "\build\dvc"

[Setup]
AppId={{8258CE8A-110E-4E0D-AE60-FEE00B15F041}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={code:GetDefaultDirName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
LicenseFile=..\..\LICENSE
OutputBaseFilename=dvc-{#MyAppVersion}
Compression=lzma2/max
SolidCompression=yes
OutputDir=.
ChangesEnvironment=yes
SetupIconFile=dvc.ico
WizardSmallImageFile=dvc_up.bmp
WizardImageFile=dvc_left.bmp
DisableWelcomePage=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "{#MyAppDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Tasks]
Name: modifypath; Description: Adds dvc's application directory to environmental path; Flags: checkablealone;
Name: modifypath\system; Description: Adds dvc's application directory to enviromental path for all users;
Name: addsymlinkpermissions; Description: Add permission for creating symbolic links; Flags: checkablealone;
Name: addsymlinkpermissions\system; Description: Add permissions for creating symbolic links for all users;

[Code]
const
	ModPathName = 'modifypath';
	ModPathPath = '{app}';
	SymLinkName = 'addsymlinkpermissions';

var
	ModPathType: String;
	SymLinkType: String;

function GetDefaultDirName(Dummy: string): string;
begin
	if IsAdminLoggedOn then begin
		Result := ExpandConstant('{pf}\{#MyAppName}');
	end else begin
		Result := ExpandConstant('{userpf}\{#MyAppName}');
	end;
end;

#include "modpath.iss"
#include "addsymlink.iss"

procedure CurStepChanged(CurStep: TSetupStep);
begin
	if CurStep = ssPostInstall then begin
		if IsTaskSelected(ModPathName + '\system') then begin
			ModPathType := 'system';
		end else begin
			ModPathType := 'user';
		end;

		if IsTaskSelected(SymLinkName + '\system') then begin
			SymLinkType := 'system';
		end else begin
			SymLinkType := 'user';
		end;

		if IsTaskSelected(ModPathName) then
			ModPath();
		if IsTaskSelected(SymLinkName) then
			AddSymLink();
	end;
end;
