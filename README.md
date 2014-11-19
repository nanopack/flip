**This is a work in progess, as [luvit](https://luvit.io) is evolving its api, flip will have to adjust to match.**

#Flip
Flip manages flipping data between nodes in a cluster. It has been written to be a replacement to vippy, an ip management utility for virtual ip failover. Flip is much more extenable then [vippy](https://github.com/postwait/vippy) though, and probably is closer to a pre-release version of [serf](https://github.com/hashicorp/serf).

#Example
Lets look at how Flip handles virtual ip failover.

Here is a an example config file:
```
{
	"id" : "flip"
	,"quorum" : 2
	,"gossip_interval" : 1000
	,"key" : "secret"
	,"ping_per_interval" : 3
	,"ping_timeout" : 1500
	,"log_level" : "info"
	,"servers" : 
		{"flip" : {"ip" : "127.0.0.1","port" : 2200, "systems": ["ip"]}
		,"flip1" : {"priority" : 1,"ip" : "127.0.0.1","port" : 2201, "systems": ["ip"]}
		,"flip2" : {"ip" : "127.0.0.1","port" : 2202}}
	,"cluster" : 
		{"config": {}
		,"system":
			{"ip":
				{"alive" : "ip_up"
				,"down" : "ip_down"
				,"type" : "shard"
				,"config" : {"interface" : "lo0"}
				,"data" : ["192.168.0.2","192.168.0.1"]}}}
}
```

##All nodes active
So using the above config file the data will be split between the nodes as follows:

**flip** | **flip1** | **flip2**
--- | --- | ---
192.168.0.1 | 192.168.0.2 |

The nodes will begin to activly probe each other and until they can detect and agree that anything has changed, the data will stay where is it.

##One node down
Lets say that flip1 fails. flip and flip2 will send a probe, not get a reply, and then start sending probes to each other to notify that flip1 is probably down. When they can agree that flip1 is down, they redivide the data between the nodes as follows:

**flip** | **flip1** | **flip2**
--- | --- | ---
192.168.0.1 | x |
192.168.0.2 | |

The ip moved over to the other node that is the member of the 'ip' system, which is what we needed. When the nodes comes back online, it will start pinging the other nodes and the ips will be divided between the nodes again.

**flip** | **flip1** | **flip2**
--- | --- | ---
192.168.0.1 | 192.168.0.2 |

##Network partition

What would happen if there was a network partition and none of the nodes could talk to each other? None of the nodes could talk to each other, so they can't agree that anyone is down, so nothing moves.

**flip** | **flip1** | **flip2**
--- | --- | ---
192.168.0.1 | 192.168.0.2 |

That is a brief explanation of what happens in a failover situation.

##Config file options
There are a few things here that we need to look at, the first is the cluster section.

##Cluster
The cluster section is where all the flipping magic happens. Here are the options that can be set and what they mean:

- config - this is the config section for data that 
- system -  this is a description of all the different systems that are part of this cluster. Every system has a key, or id, and a config sections

##System

- alive - this is the script that is run when a peice of data is assigned to this node.
- down - this is opposite of the 'alive' script, it is run when a peice of data is removed from this node.
- type - this defines how the data is split up between nodes, currently there are two options: 'replicated' and 'sharded'
- config - the is the config data that will be passed to the 'alive' and 'down' scripts.
- data - this is the data that will be divided between nodes. Currently this can only be a list of strings.

##Servers
The next part is the 'servers' section. Each server has an id, an ip and port combo, and a list of systems that will be on the node. If a system is not in the systems list for the node, for example flip2, the node will not be responsible for any data in the system, and it will be considered a membership arbiter.

- ip - the ip where the node is located
- port - the port where flip is listening 
- system - a list of systems to install on the node
- priority - a lower priority affects how data is assigned. data points will be assigned to nodes with a lower priority before other nodes, defaults to `+infinity`

##Other config options
Flip has other parameters that can be enabled to tune it to your specific application.

- id - the id of this node, the node MUST also exist in the 'servers' section, this is manditory
- quorum - how many member MUST agree before a change is made. This is optional and defaults to `math.floor(#servers/2) + 1`
- gossip_interval - this is how often flip will send a ping to the other server in the cluster
- key - this is a key used to identify flip groups, all nodes of the cluster must use the same key
- ping_per_interval - this is how many nodes will be pinged per gossip_interval
- ping_timeout - how long to wait without receiving a response before flagging the node as 'probably_down' and sending probes to other nodes
- log_level - what level of logging to enable, valid levels are: 'debug','info','warning','error', and 'fatal'


#Future work
 - Allow nodes to be dynamically added/removed to the cluster without having to reprovision all nodes in the cluster.
 - Allow data to be added/removed to the system without having to reprovision all nodes in the cluster.

If anyone wants to work on any issues or this future work, I will accept pull requests that have tests written and that move flip closer to being feature complete.