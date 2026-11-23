import json,sys,os,subprocess,re,glob
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
w=int(sys.argv[1]); nw=int(sys.argv[2]); tree=f'{S}/mut/w{w}'
env0=dict(os.environ); env0['PATH']=os.path.expanduser('~/go/bin')+':'+env0['PATH']
LAY={'broker':['go','test','-count=1','-v','./internal/broker','./internal/core','./internal/wire/...','./internal/system/...','./cmd/mdsbroker'],
 'controlplane':['go','test','-count=1','-v','./internal/controlplane/...','./internal/runtime/...','./client/...','./cmd/mdscontrolplane']}
d=json.load(open(f'{S}/mutants.json')); M={m['id']:m for v in d.values() for m in v}
res=[json.loads(l) for f in glob.glob(S+'/mut_results_w*.jsonl') for l in open(f)]
todo=[r for r in res if r['status']=='ok' and r['layers'].get('unit',{}).get('killed') and not r['layers']['unit']['buildfail']]
todo=[r for i,r in enumerate(sorted(todo,key=lambda r:r['id'])) if i%nw==w]
outf=open(f'{S}/mut_names_w{w}.jsonl','a')
done={json.loads(l)['id'] for f in glob.glob(S+'/mut_names_w*.jsonl') for l in open(f)}
for r in todo:
    if r['id'] in done: continue
    m=M[r['id']]; mod=m['module']; env=dict(env0); path=None
    if m['kind']=='bug': env['MDS_FORCE_BUG']=str(m['bug'])
    else:
        path=f"{tree}/{m['file']}"; orig=open(path).read()
        if m['kind']=='line':
            lines=orig.split('\n'); k=next(i for i in range(m['line']-1,m['line']+9) if lines[i]==m['old']); lines[k]=m['new']; new='\n'.join(lines)
        else: new=orig.replace(m['old'],m['new'])
        open(path,'w').write(new)
    try:
        p=subprocess.run(LAY[mod],cwd=f'{tree}/{mod}',env=env,capture_output=True,text=True,timeout=400)
        out=p.stdout+p.stderr
        # test names with package: track "FAIL\tpkg" and --- FAIL lines
        fails=sorted(set(re.findall(r'^\s*--- FAIL: (Test\w+|Fuzz\w+)',out,re.M)))
        panics=len(re.findall(r'^panic:',out,re.M))
    finally:
        if path: open(path,'w').write(orig)
    outf.write(json.dumps({'id':r['id'],'tests':fails,'panics':panics})+'\n'); outf.flush()
