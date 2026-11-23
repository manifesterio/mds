import subprocess,re,collections
files=subprocess.check_output(['git','ls-files'],text=True).split('\n')
files=[f for f in files if f and not f.endswith('.pb.go') and not f.endswith('.sum')]
def n(f):
    try: return sum(1 for _ in open(f,errors='ignore'))
    except: return 0
comps=[
 ('dst kernel','dst/kernel/'),('dst campaign','dst/campaign/'),('dst check (spectest,faulty,core)','dst/check/'),('dst live','dst/live/'),
 ('dst examples','dst/examples/'),('dst golden','dst/golden/'),('dst cmd determinismcheck','dst/cmd/'),
 ('dst other (assert,clock*,fault,machine,rng,sched,trace,realclock,syncclock)','dst/'),
 ('shared','shared/'),
 ('broker core logic','broker/internal/broker/'),('broker core/wire/system (non-verify)','broker/internal/core/|broker/internal/wire/|broker/internal/system/'),
 ('broker sim kafka','broker/internal/verify/simulator/kafka/'),('broker sim echo','broker/internal/verify/simulator/echo/'),('broker sim cluster','broker/internal/verify/simulator/cluster/'),
 ('broker sim fakeservices (+fakenetwork,servicetest)','broker/internal/verify/simulator/fakeservices'),('broker sim other (history,truth,preset,session,span)','broker/internal/verify/simulator/'),
 ('broker speccheck conformance','broker/internal/verify/speccheck/conformance/'),('broker speccheck injectedbug','broker/internal/verify/speccheck/injectedbug/'),('broker speccheck other','broker/internal/verify/speccheck/'),
 ('broker spec','broker/spec/'),('broker test/integrationtest','broker/test/'),('broker cmd','broker/cmd/'),
 ('cp domain+replica+metadb+executor+server (core)','controlplane/internal/controlplane/'),('cp runtime','controlplane/internal/runtime/'),
 ('cp simulator','controlplane/internal/verify/simulator'),('cp spec','controlplane/spec/'),('cp test/conformance','controlplane/test/conformance/'),
 ('cp test/integration+s3store','controlplane/test/'),('cp cmd/cpsim','controlplane/cmd/cpsim/'),('cp client+contract+cmd+tools','controlplane/'),
 ('ops/tools riskscore','ops/tools/'),('ops scripts+mutants','ops/'),
]
tot=collections.OrderedDict()
used=set()
for name,pat in comps:
    ps=pat.split('|')
    P=T=0;fs=[]
    for f in files:
        if f in used: continue
        if any(f.startswith(p) for p in ps):
            used.add(f); 
            if f.endswith('.go'):
                if f.endswith('_test.go'): T+=n(f)
                else: P+=n(f)
            else: P+=n(f)
    print(f'{name:75s} {P:6d} {T:6d} {P+T:6d}')
