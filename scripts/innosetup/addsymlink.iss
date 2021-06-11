procedure AddSymLink();
var
    ErrorCode: Integer;
    SRCdir: String;
begin
    SRCdir := ExpandConstant('{app}');
    if isUninstaller() = false then begin
        if not ShellExec('', 'powershell.exe', '-noninteractive -windowstyle hidden -executionpolicy bypass -File "' + SRCdir + '\addSymLinkPermissions.ps1" -mytype ' + SymLinkType, '', SW_HIDE, ewWaitUntilTerminated, ErrorCode) then begin
            SuppressibleMsgBox('Failed to automatically grant SeCreateSymbolicLinkPrivilege. Please download Polsedit(www.southsoftware.com/polsedit.zip). Launch polseditx32.exe or polseeditx64.exe (depending on your Windows version), navigate to "Security Settings" -> "User Rights Assignment", add the account(s) to the list named "Create symbolic links", logoff and login back into your account. More info at https://github.com/git-for-windows/git/wiki/Symbolic-Links.', mbInformation, MB_OK, IDOK);
        end else begin
            SuppressibleMsgBox('Automatically added SeCreateSymbolicLinkPrivilege. Please logoff and login back into your account in order for the change to take effect.', mbInformation, MB_OK, IDOK);
        end;
    end;
end;
