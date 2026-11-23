import json,sys,os,subprocess,time,re,glob
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
w=int(sys.argv[1]); nw=int(sys.argv[2]); MFILE=sys.argv[3]; GROUPSEL=sys.argv[4].split(','); PFX=sys.argv[5]
tree=f'{S}/mut/w{w}'
env0=dict(os.environ); env0['PATH']=os.path.expanduser('~/go/bin')+':'+env0['PATH']
SKIP='TestCapChange|TestCachedState|TestProductionCaps|TestMutantsAreCaught'
L={
 'spec':[('spec-unit','controlplane',['go','test','-count=1','./spec']),('rebalance-unit','controlplane',['go','test','-count=1','./internal/controlplane/domain/...']),('cp-conformance','controlplane',['go','test','-count=1','-failfast','./test/conformance']),('cp-sim','controlplane',['go','test','-count=1','-failfast','./internal/verify/simulator']),('cp-cpsim','controlplane',['go','test','-count=1','-failfast','./cmd/cpsim']),('broker-unit','broker',['go','test','-count=1','./internal/broker','./internal/system/...'])],
 'objspec':[('objspec-unit','shared',['go','test','-count=1','./objectstore']),('s3conform+conformance','broker',['go','test','-count=1','./internal/verify/speccheck/...']),('fakeservices','broker',['go','test','-count=1','./internal/verify/simulator/fakeservices/...']),('simkafka','broker',['go','test','-count=1','-failfast','./internal/verify/simulator/kafka','-skip',SKIP]),('broker-unit','broker',['go','test','-count=1','./internal/broker'])],
 'infra':[
  ('parity','broker',['go','test','-count=1','./internal/verify/simulator/fakeservices/internal/servicetest']),
  ('fakeservices-unit','broker',['go','test','-count=1','./internal/verify/simulator/fakeservices','./internal/verify/simulator/fakeservices/fakenetwork']),
  ('faulty-unit','dst',['go','test','-count=1','./check/...']),
  ('simkafka','broker',['go','test','-count=1','-failfast','./internal/verify/simulator/kafka','-skip',SKIP]),
  ('echo+cluster','broker',['go','test','-count=1','./internal/verify/simulator/echo','./internal/verify/simulator/cluster','./cmd/mdssimulator']),
  ('cp-sim','controlplane',['go','test','-count=1','-failfast','./internal/verify/simulator']),
 ],
 'dst':[
  ('dst-own(no examples)','dst',['go','test','-count=1','./assert/...','./campaign/...','./check/...','./clock/...','./cmd/...','./fault/...','./golden/...','./live/...','./machine/...','./realclock/...','./rng/...','./sched/...','./syncclock/...','./trace/...','./kernel/...']),
  ('dst-examples','dst',['go','test','-count=1','./examples/...']),
  ('simkafka','broker',['go','test','-count=1','-failfast','./internal/verify/simulator/kafka','-skip',SKIP]),
  ('echo+cluster','broker',['go','test','-count=1','./internal/verify/simulator/echo','./internal/verify/simulator/cluster','./cmd/mdssimulator']),
  ('cp-sim','controlplane',['go','test','-count=1','-failfast','./internal/verify/simulator']),
  ('cp-conformance','controlplane',['go','test','-count=1','-failfast','./test/conformance']),
 ]}
def sh(cmd,cwd,timeout=400):
    try:
        p=subprocess.run(cmd,cwd=cwd,env=env0,capture_output=True,text=True,timeout=timeout); return p.returncode,p.stdout+p.stderr
    except subprocess.TimeoutExpired: return 124,'TIMEOUT'
d=json.load(open(f'{S}/{MFILE}'))
allm=d['all']
mine=allm
done={json.loads(l)['id'] for f in glob.glob(f'{S}/{PFX}_results_w*.jsonl') for l in open(f)}
mine=[m for m in mine if m['id'] not in done]
outf=open(f'{S}/{PFX}_results_w{w}.jsonl','a')
for m in mine:
    try:
        os.close(os.open(f'{S}/claims/{m["id"]}',os.O_CREAT|os.O_EXCL))
    except FileExistsError: continue
    path=f"{tree}/{m['file']}"; orig=open(path).read()
    lines=orig.split('\n'); k=next(i for i in range(m['line']-1,m['line']+9) if lines[i]==m['old'])
    lines[k]=m['new']; open(path,'w').write('\n'.join(lines))
    res={'id':m['id'],'op':m['op'],'file':m['file'],'line':m['line'],'group':m['group'],'layers':{}}
    try:
        rc,o=sh(['go','build','./...'],f"{tree}/{m['module']}")
        if rc!=0: res['status']='noncompile'
        else:
            res['status']='ok'
            for name,mod,cmd in L[m['group']]:
                t=time.time(); rc,o=sh(cmd,f'{tree}/{mod}')
                res['layers'][name]={'killed':rc!=0,'fails':sorted(set(x.split('/mds/')[-1] for x in re.findall(r'^FAIL[ \t]+(\S+)',o,re.M))),'secs':round(time.time()-t,1),'buildfail':'[build failed]' in o,'timeout':rc==124}
    finally: open(path,'w').write(orig)
    outf.write(json.dumps(res)+'\n'); outf.flush()
