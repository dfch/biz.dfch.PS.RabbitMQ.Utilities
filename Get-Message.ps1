function New-Message {

[CmdletBinding(
    SupportsShouldProcess = $false
	,
    ConfirmImpact = "Low"
	,
	HelpURI='http://dfch.biz/PS/RabbitMQ/Utilities/New-Message/'
)]
Param (
	[Parameter(Mandatory = $false, Position = 0)]
	[string] $QueueName = $biz_dfch_PS_RabbitMQ_Utilities.MQ.QueueName
	,
	[Parameter(Mandatory = $false, Position = 1)]
	[int] $WaitMilliSeconds = $biz_dfch_PS_RabbitMQ_Utilities.MQ.WaitMilliSeconds
	,
	[ValidateSet('default', 'json', 'json-pretty', 'xml', 'xml-pretty', 'json-decoded')]
	[Parameter(Mandatory = $false)]
	[alias("ReturnFormat")]
	[string] $As = 'default'
) # Param

BEGIN {

$datBegin = [datetime]::Now;
[string] $fn = $MyInvocation.MyCommand.Name;
Log-Debug -fn $fn -msg ("CALL. QueueName '{0}'. WaitMilliSeconds '{1}'." -f $QueueName, $WaitMilliSeconds) -fac 1;

} # BEGIN
PROCESS {

# Default test variable for checking function response codes.
[Boolean] $fReturn = $false;
# Return values are always and only returned via OutputParameter.
$OutputParameter = $null;

try {

	$r = $biz_dfch_PS_RabbitMQ_Utilities.MQ.Receive($QueueName, $WaitMilliSeconds)
	switch($As) {
	'xml' { $OutputParameter = (ConvertTo-Xml -InputObject $r).OuterXml; }
	'xml-pretty' { $OutputParameter = Format-Xml -String (ConvertTo-Xml -InputObject $r).OuterXml; }
	'json' { $OutputParameter = ConvertTo-Json -InputObject $r -Compress; }
	'json-pretty' { $OutputParameter = ConvertTo-Json -InputObject $r; }
	'json-decoded' { $OutputParameter = ConvertFrom-Json -InputObject $r; }
	Default { $OutputParameter = $r; }
	} # switch
	$fReturn = $true;

} # try
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
		
		if($_.Exception -is [System.Net.WebException]) {
			Log-Critical $fn ("[WebException] Request FAILED with Status '{0}'. [{1}]." -f $_.Status, $_);
			Log-Debug $fn $ErrorText -fac 3;
		} # [System.Net.WebException]
		else {
			Log-Error $fn $ErrorText -fac 3;
			if($gotoError -eq $_.Exception.Message) {
				Log-Error $fn $e.Exception.Message;
				$PSCmdlet.ThrowTerminatingError($e);
			} elseif($gotoFailure -ne $_.Exception.Message) { 
				Write-Verbose ("$fn`n$ErrorText"); 
			} else {
				# N/A
			} # if
		} # other exceptions
		$fReturn = $false;
		$OutputParameter = $null;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally

} # PROCESS

END {

$datEnd = [datetime]::Now;
Log-Debug -fn $fn -msg ("RET. fReturn: [{0}]. Execution time: [{1}]ms. Started: [{2}]." -f $fReturn, ($datEnd - $datBegin).TotalMilliseconds, $datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz')) -fac 2;

# Return values are always and only returned via OutputParameter.
return $OutputParameter;

} # END

} # New-Message
if($MyInvocation.ScriptName) { Export-ModuleMember -Function New-Message; } 

<#
2014-11-24; rrink; CHG: Cmdlet is now in separate file
2014-08-24; rrink; ADD: Get-Message.
#>

# SIG # Begin signature block
# MIILewYJKoZIhvcNAQcCoIILbDCCC2gCAQExCzAJBgUrDgMCGgUAMGkGCisGAQQB
# gjcCAQSgWzBZMDQGCisGAQQBgjcCAR4wJgIDAQAABBAfzDtgWUsITrck0sYpfvNR
# AgEAAgEAAgEAAgEAAgEAMCEwCQYFKw4DAhoFAAQUwu0bZfcjO9foOukh+IsNftn+
# 5HegggjdMIIEKDCCAxCgAwIBAgILBAAAAAABL07hNVwwDQYJKoZIhvcNAQEFBQAw
# VzELMAkGA1UEBhMCQkUxGTAXBgNVBAoTEEdsb2JhbFNpZ24gbnYtc2ExEDAOBgNV
# BAsTB1Jvb3QgQ0ExGzAZBgNVBAMTEkdsb2JhbFNpZ24gUm9vdCBDQTAeFw0xMTA0
# MTMxMDAwMDBaFw0xOTA0MTMxMDAwMDBaMFExCzAJBgNVBAYTAkJFMRkwFwYDVQQK
# ExBHbG9iYWxTaWduIG52LXNhMScwJQYDVQQDEx5HbG9iYWxTaWduIENvZGVTaWdu
# aW5nIENBIC0gRzIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCyTxTn
# EL7XJnKrNpfvU79ChF5Y0Yoo/ENGb34oRFALdV0A1zwKRJ4gaqT3RUo3YKNuPxL6
# bfq2RsNqo7gMJygCVyjRUPdhOVW4w+ElhlI8vwUd17Oa+JokMUnVoqni05GrPjxz
# 7/Yp8cg10DB7f06SpQaPh+LO9cFjZqwYaSrBXrta6G6V/zuAYp2Zx8cvZtX9YhqC
# VVrG+kB3jskwPBvw8jW4bFmc/enWyrRAHvcEytFnqXTjpQhU2YM1O46MIwx1tt6G
# Sp4aPgpQSTic0qiQv5j6yIwrJxF+KvvO3qmuOJMi+qbs+1xhdsNE1swMfi9tBoCi
# dEC7tx/0O9dzVB/zAgMBAAGjgfowgfcwDgYDVR0PAQH/BAQDAgEGMBIGA1UdEwEB
# /wQIMAYBAf8CAQAwHQYDVR0OBBYEFAhu2Lacir/tPtfDdF3MgB+oL1B6MEcGA1Ud
# IARAMD4wPAYEVR0gADA0MDIGCCsGAQUFBwIBFiZodHRwczovL3d3dy5nbG9iYWxz
# aWduLmNvbS9yZXBvc2l0b3J5LzAzBgNVHR8ELDAqMCigJqAkhiJodHRwOi8vY3Js
# Lmdsb2JhbHNpZ24ubmV0L3Jvb3QuY3JsMBMGA1UdJQQMMAoGCCsGAQUFBwMDMB8G
# A1UdIwQYMBaAFGB7ZhpFDZfKiVAvfQTNNKj//P1LMA0GCSqGSIb3DQEBBQUAA4IB
# AQAiXMXdPfQLcNjj9efFjgkBu7GWNlxaB63HqERJUSV6rg2kGTuSnM+5Qia7O2yX
# 58fOEW1okdqNbfFTTVQ4jGHzyIJ2ab6BMgsxw2zJniAKWC/wSP5+SAeq10NYlHNU
# BDGpeA07jLBwwT1+170vKsPi9Y8MkNxrpci+aF5dbfh40r5JlR4VeAiR+zTIvoSt
# vODG3Rjb88rwe8IUPBi4A7qVPiEeP2Bpen9qA56NSvnwKCwwhF7sJnJCsW3LZMMS
# jNaES2dBfLEDF3gJ462otpYtpH6AA0+I98FrWkYVzSwZi9hwnOUtSYhgcqikGVJw
# Q17a1kYDsGgOJO9K9gslJO8kMIIErTCCA5WgAwIBAgISESFgd9/aXcgt4FtCBtsr
# p6UyMA0GCSqGSIb3DQEBBQUAMFExCzAJBgNVBAYTAkJFMRkwFwYDVQQKExBHbG9i
# YWxTaWduIG52LXNhMScwJQYDVQQDEx5HbG9iYWxTaWduIENvZGVTaWduaW5nIENB
# IC0gRzIwHhcNMTIwNjA4MDcyNDExWhcNMTUwNzEyMTAzNDA0WjB6MQswCQYDVQQG
# EwJERTEbMBkGA1UECBMSU2NobGVzd2lnLUhvbHN0ZWluMRAwDgYDVQQHEwdJdHpl
# aG9lMR0wGwYDVQQKDBRkLWZlbnMgR21iSCAmIENvLiBLRzEdMBsGA1UEAwwUZC1m
# ZW5zIEdtYkggJiBDby4gS0cwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
# AQDTG4okWyOURuYYwTbGGokj+lvBgo0dwNYJe7HZ9wrDUUB+MsPTTZL82O2INMHp
# Q8/QEMs87aalzHz2wtYN1dUIBUaedV7TZVme4ycjCfi5rlL+p44/vhNVnd1IbF/p
# xu7yOwkAwn/iR+FWbfAyFoCThJYk9agPV0CzzFFBLcEtErPJIvrHq94tbRJTqH9s
# ypQfrEToe5kBWkDYfid7U0rUkH/mbff/Tv87fd0mJkCfOL6H7/qCiYF20R23Kyw7
# D2f2hy9zTcdgzKVSPw41WTsQtB3i05qwEZ3QCgunKfDSCtldL7HTdW+cfXQ2IHIt
# N6zHpUAYxWwoyWLOcWcS69InAgMBAAGjggFUMIIBUDAOBgNVHQ8BAf8EBAMCB4Aw
# TAYDVR0gBEUwQzBBBgkrBgEEAaAyATIwNDAyBggrBgEFBQcCARYmaHR0cHM6Ly93
# d3cuZ2xvYmFsc2lnbi5jb20vcmVwb3NpdG9yeS8wCQYDVR0TBAIwADATBgNVHSUE
# DDAKBggrBgEFBQcDAzA+BgNVHR8ENzA1MDOgMaAvhi1odHRwOi8vY3JsLmdsb2Jh
# bHNpZ24uY29tL2dzL2dzY29kZXNpZ25nMi5jcmwwUAYIKwYBBQUHAQEERDBCMEAG
# CCsGAQUFBzAChjRodHRwOi8vc2VjdXJlLmdsb2JhbHNpZ24uY29tL2NhY2VydC9n
# c2NvZGVzaWduZzIuY3J0MB0GA1UdDgQWBBTwJ4K6WNfB5ea1nIQDH5+tzfFAujAf
# BgNVHSMEGDAWgBQIbti2nIq/7T7Xw3RdzIAfqC9QejANBgkqhkiG9w0BAQUFAAOC
# AQEAB3ZotjKh87o7xxzmXjgiYxHl+L9tmF9nuj/SSXfDEXmnhGzkl1fHREpyXSVg
# BHZAXqPKnlmAMAWj0+Tm5yATKvV682HlCQi+nZjG3tIhuTUbLdu35bss50U44zND
# qr+4wEPwzuFMUnYF2hFbYzxZMEAXVlnaj+CqtMF6P/SZNxFvaAgnEY1QvIXI2pYV
# z3RhD4VdDPmMFv0P9iQ+npC1pmNLmCaG7zpffUFvZDuX6xUlzvOi0nrTo9M5F2w7
# LbWSzZXedam6DMG0nR1Xcx0qy9wYnq4NsytwPbUy+apmZVSalSvldiNDAfmdKP0S
# CjyVwk92xgNxYFwITJuNQIto4zGCAggwggIEAgEBMGcwUTELMAkGA1UEBhMCQkUx
# GTAXBgNVBAoTEEdsb2JhbFNpZ24gbnYtc2ExJzAlBgNVBAMTHkdsb2JhbFNpZ24g
# Q29kZVNpZ25pbmcgQ0EgLSBHMgISESFgd9/aXcgt4FtCBtsrp6UyMAkGBSsOAwIa
# BQCgeDAYBgorBgEEAYI3AgEMMQowCKACgAChAoAAMBkGCSqGSIb3DQEJAzEMBgor
# BgEEAYI3AgEEMBwGCisGAQQBgjcCAQsxDjAMBgorBgEEAYI3AgEVMCMGCSqGSIb3
# DQEJBDEWBBR0XWPKkNQIvd8y0Ow/NV2pxlEDvTANBgkqhkiG9w0BAQEFAASCAQBX
# lkZ0Ogh09SIZRqdOoieE/POsKGb4qbzC0khyEa3xHfLCACFV/B1UOBQ9XR3c0kGQ
# CtIUK/UXFFW1CTkeOKVF/+xmM7xURhgalIJ8gGfzgmu+LtNEFWlYuWSHpF2Ytb6z
# HrURg172mEqtL8C3xX3Y6fNLvczP3wzyMIRFFFR+sTNNnIwSU4mPExKPUvyuxLxC
# E5MwzGPcNHAPN/G8q2geehQ63AeTde8NWOh864ygKNSmYRryppLQKl8TJXAFz3qm
# 1w/vZTzML+OzEWSc6OSvJzXikekKNhWTBepCImyy4OkQjtNXK8qXgnl2tHTbfaz+
# kbBxGJUhcJGhwo18HuZp
# SIG # End signature block
