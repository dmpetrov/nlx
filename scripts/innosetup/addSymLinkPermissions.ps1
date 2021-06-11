param (
	[string]$mytype = 'system'
)

function addSymLinkPermissions($accountToAdd){
    Write-Host "Checking SymLink permissions.."
    $sidstr = $null
    if ( "$accountToAdd" -eq "Everyone" ) {
    	$sidstr = "S-1-1-0"
    } else {
        try {
            $ntprincipal = new-object System.Security.Principal.NTAccount "$accountToAdd"
            $sid = $ntprincipal.Translate([System.Security.Principal.SecurityIdentifier])
            $sidstr = $sid.Value.ToString()
        } catch {
            $sidstr = $null
        }
    }
    Write-Host "Account: $($accountToAdd)" -ForegroundColor DarkCyan
    if( [string]::IsNullOrEmpty($sidstr) ) {
        Write-Host "Account not found!" -ForegroundColor Red
        exit -1
    }
    Write-Host "Account SID: $($sidstr)" -ForegroundColor DarkCyan
    $tmp = [System.IO.Path]::GetTempFileName()
    Write-Host "Export current Local Security Policy" -ForegroundColor DarkCyan
    secedit.exe /export /cfg "$($tmp)" 
    $c = Get-Content -Path $tmp 
    $currentSetting = ""
    foreach($s in $c) {
        if( $s -like "SECreateSymbolicLinkPrivilege*") {
            $x = $s.split("=",[System.StringSplitOptions]::RemoveEmptyEntries)
            $currentSetting = $x[1].Trim()
        }
    }
    if( $currentSetting -notlike "*$($sidstr)*" ) {
        Write-Host "Need to add permissions to SymLink" -ForegroundColor Yellow
        
        Write-Host "Modify Setting ""Create SymLink""" -ForegroundColor DarkCyan

        if( [string]::IsNullOrEmpty($currentSetting) ) {
            $currentSetting = "*$($sidstr)"
        } else {
            $currentSetting = "*$($sidstr),$($currentSetting)"
        }
        Write-Host "$currentSetting"
    $outfile = @"
[Unicode]
Unicode=yes
[Version]
signature="`$CHICAGO`$"
Revision=1
[Privilege Rights]
SECreateSymbolicLinkPrivilege = $($currentSetting)
"@
    $tmp2 = [System.IO.Path]::GetTempFileName()
        Write-Host "Import new settings to Local Security Policy" -ForegroundColor DarkCyan
        $outfile | Set-Content -Path $tmp2 -Encoding Unicode -Force
        Push-Location (Split-Path $tmp2)
        try {
            secedit.exe /configure /db "secedit.sdb" /cfg "$($tmp2)" /areas USER_RIGHTS 
        } finally { 
            Pop-Location
        }
    } else {
        Write-Host "NO ACTIONS REQUIRED! Account already in ""Create SymLink""" -ForegroundColor DarkCyan
        Write-Host "Account $accountToAdd already has permissions to SymLink" -ForegroundColor Green
        return $true;
    }
}

if ( "$mytype" -eq "user" ) {
    addSymLinkPermissions $(Get-WMIObject -class Win32_ComputerSystem | select username).username
} else {
    addSymLinkPermissions Everyone
}
