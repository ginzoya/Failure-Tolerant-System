'''
  Generate publish and subscribe ports for the instances and any proxies.

  Call this function generates in every database instance and proxy instance.
  It will generate consistent port numbers for them all, ensuring correct
  interconnection.
'''

def gen_ports(base, instances, proxied_instances, this_instance, proxy=False):
    '''
      Return the publish port and the subsribe port(s) for this instance
      or its proxy.

      base: Base port number. All generated ports will be consecutive
            values >= the base
      instances: List of strings naming the instances.  Each string
            must be unique.
      proxied_instances: List of zero or more instance names that
            should be proxied. All names in this list should
            appear also in the instances list.
      this_instance: Name of the instance for which the ports
            should be generated or for which proxy ports should
            be generated. Must be in instances.
      proxy: If True, generate ports for the proxy for this_instance.
      
      Returns: 
        If proxy == False: Returns publish port and list of 0 or more ports
            to which the instance should subscribe.
        If proxy == True: Returns publish port and exactly one port to which
            the proxy for this instance should subscribe.
    '''
    assert base > 1023
    
    assert len(set(instances)) == len(instances)
    assert len(instances) > 0
    
    assert len(set(proxied_instances)) == len(proxied_instances)
    for pdb in proxied_instances:
        assert pdb in instances
        
    assert this_instance in instances
    
    assert not proxy or this_instance in proxied_instances

    pub_ports = range(base, base+len(instances))
    pub_proxy_pub = [-1] * len(instances)

    sub_ports = [[]] * len(instances)
    for i in range(len(instances)):
        sub_ports[i] = range(base, base+len(instances))
        sub_ports[i][i] = -1

    next_proxy = base + len(instances)
    for pdb in proxied_instances:
        index = instances.index(pdb)
        pub_proxy_pub[index] = next_proxy
        for i in range(len(instances)):
            if i != index:
                sub_ports[i][index] = next_proxy
        next_proxy += 1

    this_ind = instances.index(this_instance)
    if not proxy:
        return pub_ports[this_ind], [subp for subp in sub_ports[this_ind] if subp != -1]
    else:
        return pub_proxy_pub[this_ind],pub_ports[this_ind]
