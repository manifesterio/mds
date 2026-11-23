import json,glob,os,subprocess,re,sys
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
w=int(sys.argv[1]); nw=int(sys.argv[2]); tree=f'{S}/mut/w{w}'
env=dict(os.environ); env['PATH']=os.path.expanduser('~/go/bin')+':'+env['PATH']
d=json.load(open(S+'/mutants4.json'))['all']; ms=[m for m in d if m['group']=='spec']
out=open(f'{S}/spec_extra_w{w}.jsonl','a')
for i,m in enumerate(ms):
    if i%nw!=w-3: continue
    path=f"{tree}/{m['file']}"; orig=open(path).read()
    lines=orig.split('\n'); k=next(j for j in range(m['line']-1,m['line']+9) if lines[j]==m['old']); lines[k]=m['new']; open(path,'w').write('\n'.join(lines))
    try:
        p=subprocess.run(['go','test','-count=1','./internal/verify/speccheck/conformance','./internal/verify/speccheck/injectedbug'],cwd=tree+'/broker',env=env,capture_output=True,text=True,timeout=400)
        o=p.stdout+p.stderr
        out.write(json.dumps({'id':m['id'],'killed':p.returncode!=0,'build':'[build failed]' in o,'fails':re.findall(r'^FAIL[ \t]+(\S+)',o,re.M)})+'\n'); out.flush()
    finally: open(path,'w').write(orig)
