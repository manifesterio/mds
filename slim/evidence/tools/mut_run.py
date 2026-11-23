import json,sys,os,subprocess,time,re
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
w=int(sys.argv[1]); nw=int(sys.argv[2]); only=sys.argv[3] if len(sys.argv)>3 else None
tree=f'{S}/mut/w{w}'
env0=dict(os.environ); env0['PATH']=os.path.expanduser('~/go/bin')+':'+env0['PATH']
LAYERS={
 'broker':[
  ('unit',['go','test','-count=1','./internal/broker','./internal/core','./internal/wire/...','./internal/system/...','./cmd/mdsbroker']),
  ('simkafka',['go','test','-count=1','-failfast','./internal/verify/simulator/kafka','-skip','TestCapChange|TestCachedState|TestProductionCaps|TestMutantsAreCaught']),
  ('simother',['go','test','-count=1','./internal/verify/simulator/echo','./internal/verify/simulator/cluster','./internal/verify/simulator/fakeservices/...','./cmd/mdssimulator']),
 ],
 'controlplane':[
  ('unit',['go','test','-count=1','./internal/controlplane/...','./internal/runtime/...','./client/...','./cmd/mdscontrolplane']),
  ('sim',['go','test','-count=1','-failfast','./internal/verify/simulator']),
  ('conformance',['go','test','-count=1','-failfast','./test/conformance']),
  ('cpsim',['go','test','-count=1','-failfast','./cmd/cpsim']),
 ]}
def sh(cmd,cwd,env=None,timeout=300):
    try:
        p=subprocess.run(cmd,cwd=cwd,env=env or env0,capture_output=True,text=True,timeout=timeout)
        return p.returncode,p.stdout+p.stderr
    except subprocess.TimeoutExpired as e:
        return 124,'TIMEOUT'
d=json.load(open(f'{S}/mutants.json'))
allm=[m for mod in ('controlplane','broker') for m in d[mod]]
mine=[m for i,m in enumerate(allm) if i%nw==w and (only is None or m['module']==only)]
import glob
done={json.loads(l)['id'] for f in glob.glob(f'{S}/mut_results_w*.jsonl') for l in open(f)}
mine=[m for m in mine if m['id'] not in done]
outf=open(f'{S}/mut_results_w{w}.jsonl','a')
for m in mine:
    mod=m['module']; mdir=f'{tree}/{mod}'
    env=dict(env0)
    path=None
    if m['kind']=='bug':
        env['MDS_FORCE_BUG']=str(m['bug'])
    else:
        path=f"{tree}/{m['file']}"
        orig=open(path).read()
        if m['kind']=='line':
            lines=orig.split('\n'); k=next(i for i in range(m['line']-1,m['line']+9) if lines[i]==m['old'])
            lines[k]=m['new']; new='\n'.join(lines)
        else:
            m['old']=m['old'].rstrip('\n'); m['new']=m['new'].rstrip('\n'); assert orig.count(m['old'])==1,m['id']
            new=orig.replace(m['old'],m['new'])
        open(path,'w').write(new)
    res={'id':m['id'],'op':m['op'],'file':m.get('file',''),'line':m.get('line',0),'layers':{}}
    try:
        rc,o=sh(['go','build','./...'],mdir,env)
        if rc!=0:
            res['status']='noncompile'; res['err']=o[-200:]
        else:
            res['status']='ok'
            for name,cmd in LAYERS[mod]:
                t=time.time(); rc,o=sh(cmd,mdir,env)
                fails=sorted(set(re.findall(r'^FAIL[ \t]+(\S+)',o,re.M)))
                bf='[build failed]' in o or '[setup failed]' in o
                res['layers'][name]={'killed':rc!=0,'fails':[f.split('/mds/')[-1] for f in fails],'secs':round(time.time()-t,1),'buildfail':bf,'timeout':rc==124}
    finally:
        if path: open(path,'w').write(orig)
    outf.write(json.dumps(res)+'\n'); outf.flush()
