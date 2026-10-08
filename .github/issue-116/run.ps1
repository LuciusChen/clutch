# Reproduction driver for issue 116: PostgreSQL statement vs display-line-numbers-mode on Windows.
param([string]$EmacsDir, [string]$ResultFile)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms, System.Drawing
Add-Type @'
using System; using System.Runtime.InteropServices;
public class Win { [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h, int c); }
'@
$ec = Join-Path $EmacsDir 'bin\emacsclient.exe'
function Ev([string]$form) { (& $ec -e $form 2>&1 | Out-String).Trim() }
function Log([string]$line) { Write-Host $line; Add-Content -Path $ResultFile -Value $line }
function Focus {
  $p = Get-Process emacs -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1
  if ($p) { [Win]::ShowWindow($p.MainWindowHandle, 9) | Out-Null; [Win]::SetForegroundWindow($p.MainWindowHandle) | Out-Null }
  Start-Sleep -Milliseconds 500
}
function Wait-State([double]$seconds, [string]$cancelKeys, [double]$cancelAt) {
  $sw = [Diagnostics.Stopwatch]::StartNew(); $cancelled = $false
  while ($sw.Elapsed.TotalSeconds -lt $seconds) {
    if ((Ev '(t116-state)') -eq '"result"') { return ('result after {0:N2}s' -f $sw.Elapsed.TotalSeconds) }
    if ($cancelKeys -and -not $cancelled -and $sw.Elapsed.TotalSeconds -gt $cancelAt) {
      Focus; [Windows.Forms.SendKeys]::SendWait($cancelKeys); $cancelled = $true
      Write-Host ('  sent cancel keys at {0:N2}s' -f $sw.Elapsed.TotalSeconds)
    }
    Start-Sleep -Milliseconds 200
  }
  return "NO RESULT after $seconds s"
}

Log "== $(Ev '(t116-info)')"
Ev '(t116-console)' | Out-Null
$sw = [Diagnostics.Stopwatch]::StartNew()
while ((Ev '(t116-ready)') -ne 't' -and $sw.Elapsed.TotalSeconds -lt 60) { Start-Sleep -Seconds 1 }
Log "connected: $(Ev '(t116-ready)')"

foreach ($ln in 0, 1) {
  foreach ($case in @(@{i=0; name='SELECT 1'; cancel=''; at=0; wait=8}, @{i=1; name='pg_sleep(20) then C-g at 1s'; cancel='^g'; at=1; wait=10})) {
    Ev "(t116-prep $($case.i) $ln)" | Out-Null
    Focus
    [Windows.Forms.SendKeys]::SendWait('^c^c')
    $res = Wait-State $case.wait $case.cancel $case.at
    if ($res -like 'NO RESULT*') {
      # does one wake-up key unstick it, as the reporter observed?
      Focus; [Windows.Forms.SendKeys]::SendWait('^c')
      Start-Sleep -Seconds 1
      $res += "; after one C-c: $(Ev '(t116-state)')"
      Focus; [Windows.Forms.SendKeys]::SendWait('^g')
    }
    Log ("line-numbers={0} | {1} | {2}" -f $ln, $case.name, $res)
  }
}
$bmp = New-Object Drawing.Bitmap ([Windows.Forms.SystemInformation]::VirtualScreen.Width), ([Windows.Forms.SystemInformation]::VirtualScreen.Height)
$g = [Drawing.Graphics]::FromImage($bmp); $g.CopyFromScreen(0, 0, 0, 0, $bmp.Size)
$bmp.Save((Join-Path (Split-Path $ResultFile) 'screen.png'))
