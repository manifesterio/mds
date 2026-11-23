import glob,os,collections
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
def load(p):
    d={}
    for i,l in enumerate(open(p)):
        if i==0 or not l.strip(): continue
        a=l.rsplit(' ',2); d[a[0]]=(int(a[1]),int(a[2]))
    return d
P={os.path.basename(f)[:-4]:load(f) for f in glob.glob(S+'/covdst/*.out')}
allb={}
for d in P.values():
    for k,(s,c) in d.items(): allb[k]=s
cov={n:{k for k,(s,c) in d.items() if c>0} for n,d in P.items()}
def grp(n):
    if n.startswith('dst_examples'): return 'dst-examples'
    if n.startswith('dst_'): return 'dst-own'
    if n.startswith('broker_internal_verify_speccheck') : return 'broker-speccheck'
    if n.startswith('broker_internal_verify_simulator_echo'): return 'broker-echo'
    if n.startswith('broker_internal_verify_simulator_fakeservices'): return 'broker-fakeservices'
    if n.startswith('broker_'): return 'broker-sim+unit'
    return 'cp-sim+conf+unit'
G=collections.defaultdict(set)
for n,c in cov.items(): G[grp(n)]|=c
tot=sum(allb.values())
w=lambda ks:sum(allb[k] for k in ks)
print('dst stmts',tot)
for g,c in sorted(G.items()):
    others=set().union(*[x for h,x in G.items() if h!=g])
    print(f'{g:22s} covers {w(c):5d} ({100*w(c)/tot:4.1f}%)  unique {w(c-others):5d}')
cons=G['broker-sim+unit']|G['cp-sim+conf+unit']|G['broker-echo']|G['broker-fakeservices']|G['broker-speccheck']
own=G['dst-own']; ex=G['dst-examples']
print('covered by consumers(broker+cp):',w(cons),' dst-own:',w(own),' examples:',w(ex))
print('dst-own-only (not consumers):',w(own-cons),' examples-only (not consumers, not own):',w(ex-cons-own), 'examples not consumers:',w(ex-cons))
print('not covered by anyone:',tot-w(set().union(*G.values())))
bypkg=collections.defaultdict(lambda:[0,0,0,0])
for k,s in allb.items():
    pk=os.path.dirname(k.split(':')[0]).split('/dst/')[-1]
    b=bypkg[pk]; b[0]+=s
    if k in cons: b[1]+=s
    if k in own and k not in cons: b[2]+=s
    if k not in own and k not in cons and k not in ex: b[3]+=s
print(f"{'pkg':24s} total consumers-cover own-only uncovered")
for pk,b in sorted(bypkg.items(),key=lambda x:-x[1][0]): print(f'{pk:24s} {b[0]:5d} {b[1]:5d} {b[2]:5d} {b[3]:5d}')
