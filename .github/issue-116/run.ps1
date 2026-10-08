# Reproduction driver for issue 116: PostgreSQL statement vs display-line-numbers-mode on Windows.
param([string]$EmacsDir, [string]$ResultFile, [switch]$Plain)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms, System.Drawing
Add-Type @'
using System; using System.Runtime.InteropServices;
public class Win { [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h, int c);
  [DllImport("user32.dll")] public static extern void keybd_event(byte vk, byte scan, uint flags, UIntPtr extra); }
'@
$ec = Join-Path $EmacsDir 'bin\emacsclient.exe'
function Ev([string]$form) { (& $ec -e $form 2>&1 | Out-String).Trim() }
function Log([string]$line) { Write-Host $line; Add-Content -Path $ResultFile -Value $line }
function CtrlKey([byte]$vk) {
  [Win]::keybd_event(0x11, 0, 0, [UIntPtr]::Zero)
  [Win]::keybd_event($vk, 0, 0, [UIntPtr]::Zero)
  Start-Sleep -Milliseconds 60
  [Win]::keybd_event($vk, 0, 2, [UIntPtr]::Zero)
  [Win]::keybd_event(0x11, 0, 2, [UIntPtr]::Zero)
  Start-Sleep -Milliseconds 120
}
function Focus {
  $p = Get-Process emacs -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Select-Object -First 1
  if ($p) { [Win]::ShowWindow($p.MainWindowHandle, 9) | Out-Null; [Win]::SetForegroundWindow($p.MainWindowHandle) | Out-Null }
  Start-Sleep -Milliseconds 500
}
function Wait-State([double]$seconds, [int]$cancelVk, [double]$cancelAt) {
  $sw = [Diagnostics.Stopwatch]::StartNew(); $cancelled = $false
  while ($sw.Elapsed.TotalSeconds -lt $seconds) {
    if ((Ev '(t116-state)') -eq '"result"') { return ('result after {0:N2}s' -f $sw.Elapsed.TotalSeconds) }
    if ($cancelVk -and -not $cancelled -and $sw.Elapsed.TotalSeconds -gt $cancelAt) {
      Focus; CtrlKey $cancelVk; $cancelled = $true
      Write-Host ('  sent cancel keys at {0:N2}s' -f $sw.Elapsed.TotalSeconds)
    }
    Start-Sleep -Milliseconds 200
  }
  return "NO RESULT after $seconds s"
}

Log "== $(Ev '(t116-info)') plain=$Plain"
if ($Plain) {
  Ev '(t116-use-plain)' | Out-Null
  Focus
  CtrlKey 0x43; CtrlKey 0x45                      # C-c C-e
  Start-Sleep -Seconds 1
  foreach ($vk in 0x53, 0x48, 0x4F, 0x50) {       # s h o p
    [Win]::keybd_event($vk, 0, 0, [UIntPtr]::Zero); [Win]::keybd_event($vk, 0, 2, [UIntPtr]::Zero); Start-Sleep -Milliseconds 80
  }
  [Win]::keybd_event(0x0D, 0, 0, [UIntPtr]::Zero); [Win]::keybd_event(0x0D, 0, 2, [UIntPtr]::Zero)
} else {
  Ev '(t116-console)' | Out-Null
}
$sw = [Diagnostics.Stopwatch]::StartNew()
while ((Ev '(t116-ready)') -ne 't' -and $sw.Elapsed.TotalSeconds -lt 60) { Start-Sleep -Seconds 1 }
Log "connected: $(Ev '(t116-ready)')"

foreach ($ln in 0, 1) {
  foreach ($case in @(@{i=0; name='SELECT 1'; cancel=0; at=0; wait=8}, @{i=1; name='pg_sleep(20) then C-g at 1s'; cancel=0x47; at=1; wait=10})) {
    Ev "(t116-prep $($case.i) $ln)" | Out-Null
    Focus
    Log ("  redisplay check: {0}" -f (Ev '(t116-ln-width)'))
    CtrlKey 0x43; CtrlKey 0x43
    $res = Wait-State $case.wait $case.cancel $case.at
    if ($res -like 'NO RESULT*') {
      # does one wake-up key unstick it, as the reporter observed?
      Focus; CtrlKey 0x43
      Start-Sleep -Seconds 1
      $res += "; after one C-c: $(Ev '(t116-state)')"
      Focus; CtrlKey 0x47
    }
    Log ("line-numbers={0} | {1} | {2}" -f $ln, $case.name, $res)
  }
}
$bmp = New-Object Drawing.Bitmap ([Windows.Forms.SystemInformation]::VirtualScreen.Width), ([Windows.Forms.SystemInformation]::VirtualScreen.Height)
$g = [Drawing.Graphics]::FromImage($bmp); $g.CopyFromScreen(0, 0, 0, 0, $bmp.Size)
$bmp.Save((Join-Path (Split-Path $ResultFile) 'screen.png'))
