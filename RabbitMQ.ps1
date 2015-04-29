#Requires -Module 'biz.dfch.PS.System.Logging'
#Requires -Module 'biz.dfch.PS.System.Utilities'
#Requires -Module 'biz.dfch.PS.RabbitMQ.Utilities'

$Client = $biz_dfch_PS_RabbitMQ_Utilities.MQ;

function Enter-Server{
<#
.SYNOPSIS

Connects to a RabbitMQ broker.

.DESCRIPTION

Connects to a RabbitMQ broker.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM
(
    #Defines the server
	[Parameter(Mandatory = $false, Position = 0)]
	[string] $Server = $Client.Server
    ,
    #Defines username
	[Parameter(Mandatory = $false, Position = 1)]
	[string] $Username = "guest"
    ,
    #Defines password
	[Parameter(Mandatory = $false, Position = 2)]
	[string] $Password = "guest"
    ,
    #Defines virtual host
	[Parameter(Mandatory = $false, Position = 3)]
	[string] $VirtualHost = "/"
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn ("CALL. Server '{0}'; Username: '{1}'; VirtualHost: '{2}'." -f $Server,$Username,$VirtualHost) -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {
    $Client.Server = $Server;
    if($Username){
        $Client.Username = $Username;
    }
    if($Password){
        $Client.Password = $Password;
    }
    if($VirtualHost){
        $Client.VirtualHost = $VirtualHost;
    }
    $OutputParameter = $Client.Connect()
    $fReturn = $true;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}

function New-Channel{
<#
.SYNOPSIS

Creates a new channel.

.DESCRIPTION

Creates a new channel.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM()
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn "CALL." -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {
    $OutputParameter = $Client.Connection.CreateModel();
    #set default channel if it is not set
    if(!$Client.Channel){
        $Client.Channel = $OutputParameter;
    }
    $fReturn = $true;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}

function Send-Message{
<#
.SYNOPSIS

Sends a new message to a exchange.

.DESCRIPTION

Sends a new message to a exchange.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM
(
    #Defines the message (can be null or empty)
	[Parameter(Mandatory = $true, Position = 0)]
    [AllowEmptyString()]
	[string] $Message
    ,
    #Defines the exchange name
	[Parameter(Mandatory = $true, Position = 1)]
	[string] $ExchangeName
    ,
    #Defines the routing key
	[Parameter(Mandatory = $false, Position = 2)]
	[string] $RoutingKey = ""
    ,
    #Defines the channel which should be used
	[Parameter(Mandatory = $false, Position = 3)]
	[RabbitMQ.Client.Impl.ModelBase] $Channel = $Client.Channel
    ,
    #Defines if the message should be persistent
	[Parameter(Mandatory = $false, Position = 4)]
	[boolean] $Persistent = $true
    ,
    #Enables or Disables publisher confirms
	[Parameter(Mandatory = $false, Position = 5)]
	[boolean] $PublisherConfirm = $false
    ,
    #Defines the message headers
	[Parameter(Mandatory = $false, Position = 6)]
	[object] $Headers
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn ("CALL. Message: '{0}'; ExchangeName: '{1}'; RoutingKey: '{2}'; Persistent: '{3}'; PublisherConfirm: '{4}'." -f $Message,$ExchangeName,$RoutingKey,$Persistent,$PublisherConfirm) -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {
    if($Message){
        $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Message);
    } else {
        $Bytes = $null;
    }

    if($PublisherConfirm){
        $Channel.ConfirmSelect();
    }
    #Perhaps change to parameters later
    $ChannelProperties = $Channel.CreateBasicProperties()
    $ChannelProperties.ContentType = "application/json";
    $ChannelProperties.ContentEncoding = "UTF8";
    if($Headers){
        $ChannelProperties.Headers = $Headers;
    }

    $ChannelProperties.SetPersistent($Persistent);
    $Channel.BasicPublish($ExchangeName,$RoutingKey,$ChannelProperties,$Bytes);
    if($PublisherConfirm){
        #non routable messages will be acked as soon as the broker knows he can not route them
        #to track for non routable messages the mandatory flag might have to be added to the message and BasicReturnEventHandler would be needed to implement
        $Timeout = 5000;
        $Timedout = $null;
        $Confirmed = $Channel.WaitForConfirms($Timeout,[ref]$Timedout);
        if(!$Confirmed){
            if($Timedout){
                Log-Warn $fn "Publisher confirm timed out."
            }
            $e = New-CustomErrorRecord -m "Failed to get message confirmation."
            throw($gotoError);
        }
    }       
    $fReturn = $true;
    $OutputParameter = $fReturn;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}


function New-Consumer{
<#
.SYNOPSIS

Creates a new consumer.

.DESCRIPTION

Creates a new consumer.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM
(
    #Defines the queue name
	[Parameter(Mandatory = $true, Position = 0)]
	[string] $QueueName
    ,
    #Defines the channel which should be used
	[Parameter(Mandatory = $false, Position = 1)]
	[RabbitMQ.Client.Impl.ModelBase] $Channel = $Client.Channel
    ,
    #Defines the number of prefetched messages
	[Parameter(Mandatory = $false, Position = 2)]
	[uint16] $PrefetchCount = $null
    ,
    #No acknowledge
	[Parameter(Mandatory = $false, Position = 3)]
	[boolean] $NoAck = $false
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn ("CALL. QueueName: '{0}'; PrefetchCount: '{1}'; NoAck: '{2}'." -f $QueueName,$PrefetchCount,$NoAck) -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {

    if($PrefetchCount){
        $Channel.BasicQos($null,$PrefetchCount,$false);
    }
    $Consumer = New-Object RabbitMQ.Client.QueueingBasicConsumer($Channel);
    #Caution: Do not access the queue proptery of the consumer directly - it just deadlocks
    $ConsumerTag = $Channel.BasicConsume($QueueName,$NoAck,$Consumer);
    $OutputParameter = $Consumer;
    $fReturn = $true;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}

function Receive-Message{
<#
.SYNOPSIS

Receives a message from a consumer.

.DESCRIPTION

Receives a message from a consumer.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM
(
    #Defines the channel which should be used
	[Parameter(Mandatory = $true, Position = 0)]
	[RabbitMQ.Client.DefaultBasicConsumer] $Consumer
    ,
    #Defines timeout in milliseconds. -1 waits until message is received.
	[Parameter(Mandatory = $false, Position = 1)]
	[int] $Timeout = -1
    ,
    #Defines if the message will be acknowledged automatically 
	[Parameter(Mandatory = $false, Position = 2)]
	[boolean] $AutoAck = $false
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn ("CALL. Timeout: '{0}'; AutoAck: '{1}'." -f $Timeout,$AutoAck) -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {

    #Create variable for RabbitMQ.Client.Events.BasicAckEventArgs result
    #Set to null on init to work with Dequeue() method as $BasicDeliveryEvents = new-object RabbitMQ.Client.Events.BasicAckEventArgs doesn't work.
    $BasicDeliveryEvents = $null
    #Dequeue can wait for a new message until a timeout is reached. It is defined in milliseconds. -1 = infinite timeout (same as Dequeue() without arguments)
    $Result = $Consumer.Queue.Dequeue([int]$Timeout,[ref]$BasicDeliveryEvents);
    if($AutoAck){
        if($Result){
            Log-Debug $fn "AutoAck is set and result received. Trying Ack..."
	        #not forget to ack message as soon as we are ready to do so...
	        #if the channel is not open anymore after the BasicAck. Try to check the DeliveryTag before you pass it to BasicAck
	        $Consumer.Model.BasicAck($BasicDeliveryEvents.DeliveryTag,$false);
        }
    }
    $OutputParameter = $BasicDeliveryEvents;
    $fReturn = $true;

}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}

function Send-Acknowledge{
<#
.SYNOPSIS

Manually sends acknowldege for a message.

.DESCRIPTION

Manually sends acknowldege for a message.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM
(
    #Defines the object which should be acknowledged
	[Parameter(Mandatory = $true, Position = 0)]
	[Object] $MessageObject
    ,
    #Defines the channel which should be used
	[Parameter(Mandatory = $false, Position = 1)]
	[RabbitMQ.Client.Impl.ModelBase] $Channel = $Client.Channel
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn ("CALL.") -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {

    if(($MessageObject.getType().FullName -ne 'RabbitMQ.Client.Events.BasicDeliverEventArgs') -and ($MessageObject.getType().FullName -ne 'RabbitMQ.Client.BasicGetResult')){
        $e = New-CustomErrorRecord -m "Invalid object type."
        throw($gotoError);
    }
    $Channel.BasicAck($MessageObject.DeliveryTag,$false);
    $OutputParameter = $true;
    $fReturn = $true;

}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}

function ConvertTo-Header {
<#
.SYNOPSIS

Converts a hashtable to RabbitMQ message headers.

.DESCRIPTION

Converts a hashtable to RabbitMQ message headers. Using UTF8 encoding.

RabbitMQ client converts strings in headers automatically to byte arrays.

Manually passing already converted byte arrays should prevent encoding/decoding issues.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM(
    #Defines the hashtable with the headers
	[Parameter(Mandatory = $true, Position = 0)]
	[hashtable] $Headers
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn "CALL." -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {
    $Dictionary = New-Object "System.Collections.Generic.Dictionary``2[System.String,System.Object]"
    $Headers.GetEnumerator() | ForEach-Object {
        if("String" -ne $_.Value.getType().name){
            $Value = $_.Value.toString();
        } else {
            $Value = $_.Value;
        }
        $Dictionary.Add($_.Name,[System.Text.Encoding]::UTF8.GetBytes($Value));
    }
    $OutputParameter = $Dictionary;

    $fReturn = $true;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}


function ConvertFrom-Header {
<#
.SYNOPSIS

Converts RabbitMQ message headers to a hashtable.

.DESCRIPTION

Converts RabbitMQ message headers to a hashtable. Using UTF8 encoding.

RabbitMQ client converts strings in headers automatically to byte arrays.

Manually passing already converted byte arrays should prevent encoding/decoding issues.

.EXAMPLE

.LINK

.NOTES
#>
[CmdletBinding()]
PARAM(
    #Defines the dictionary with the headers
	[Parameter(Mandatory = $true, Position = 0)]
	[object] $Headers
)
BEGIN {
	$datBegin = [datetime]::Now;
	[string] $fn = $MyInvocation.MyCommand.Name;
	Log-Debug $fn "CALL." -fac 1;
}
PROCESS {

[boolean] $fReturn = $false;
try {
    $Hashtable = @{}
    $Headers.GetEnumerator() | ForEach-Object {
        $Hashtable.Add($_.Key,[System.Text.Encoding]::UTF8.GetString($_.Value));
    }
    $OutputParameter = $Hashtable;

    $fReturn = $true;
}
catch {
	if($gotoSuccess -eq $_.Exception.Message) {
		$fReturn = $true;
	} else {
		[string] $ErrorText = "catch [$($_.FullyQualifiedErrorId)]";
		$ErrorText += (($_ | fl * -Force) | Out-String);
		$ErrorText += (($_.Exception | fl * -Force) | Out-String);
		$ErrorText += (Get-PSCallStack | Out-String);
        Log-Error $fn $ErrorText -fac 3;
		if($gotoError -eq $_.Exception.Message) {
			Log-Error $fn $e.Exception.Message;
			$PSCmdlet.ThrowTerminatingError($e);
		} elseif($gotoFailure -ne $_.Exception.Message) { 
			Write-Verbose ("$fn`n$ErrorText"); 
		} else {
			# N/A
		} # if
		# other exceptions            
		$fReturn = $false;
		$OutputParameter = $fReturn;
	} # !$gotoSuccess
} # catch
finally {
	# Clean up
	# N/A
} # finally
return $OutputParameter;
} # PROCESS
END {
	$datEnd = [datetime]::Now;
	Log-Debug -fn $fn -msg "RET. fReturn: [$fReturn]. Execution time: [$(($datEnd - $datBegin).TotalMilliseconds)]ms. Started: [$($datBegin.ToString('yyyy-MM-dd HH:mm:ss.fffzzz'))]." -fac 2;
} # END
}