import sys,os,re,glob,subprocess
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
R='/Users/arun.lakshman/workspace/manifester/mds-verify-slim'
mod,pkg=sys.argv[1],sys.argv[2]   # e.g. broker internal/broker
CORE={'broker':"github.com/manifesterio/mds/broker/internal/broker/...,github.com/manifesterio/mds/broker/internal/core/...,github.com/manifesterio/mds/broker/internal/wire/...",
      'controlplane':"github.com/manifesterio/mds/controlplane/internal/controlplane/...,github.com/manifesterio/mds/controlplane/internal/runtime/..."}[mod]
env=dict(os.environ); env['PATH']=os.path.expanduser('~/go/bin')+':'+env['PATH']
def load(p):
    d={}
    for i,l in enumerate(open(p)):
        if i==0 or not l.strip(): continue
        parts=l.rsplit(' ',2); d[parts[0]]=(int(parts[1]),int(parts[2]))
    return d
allb={}
profs={}
for f in glob.glob(f'{S}/cov/{mod}_*.out'):
    n=os.path.basename(f)[len(mod)+1:-4]; profs[n]=load(f)
    for k,(s,c) in profs[n].items(): allb[k]=s
me=pkg.replace('/','_')
others=set()
for n,d in profs.items():
    if n!=me: others|={k for k,(s,c) in d.items() if c>0}
full={k for k,(s,c) in profs[me].items() if c>0}
print(f'{mod}/{pkg}: full covers {sum(allb[k] for k in full)}; not covered by other layers: {sum(allb[k] for k in full-others)}')
d=f'{R}/{mod}/{pkg}'
for tf in sorted(glob.glob(d+'/*_test.go')):
    src=open(tf).read()
    names=re.findall(r'^func ((?:Test|Fuzz|Example)\w+)\(',src,re.M)
    lines=sum(1 for _ in open(tf))
    if not names: print(f'{os.path.basename(tf):34s} {lines:5d} lines  (helpers only)'); continue
    out=f'{S}/cov/tmp_{mod}_{me}.out'
    subprocess.run(['go','test','-count=1','-skip','^('+'|'.join(names)+')$','-coverpkg='+CORE,'-coverprofile='+out,'./'+pkg],cwd=f'{R}/{mod}',env=env,capture_output=True)
    if not os.path.exists(out): print('fail',tf); continue
    wo={k for k,(s,c) in load(out).items() if c>0}
    marg=(full-wo)-others
    marg_pkg_only=full-wo
    print(f'{os.path.basename(tf):34s} {lines:5d} lines  tests={len(names):3d}  marginal-vs-world={sum(allb[k] for k in marg):4d} stmts   marginal-vs-pkg={sum(allb[k] for k in marg_pkg_only):4d}')
