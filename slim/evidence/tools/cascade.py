import subprocess,re,sys,os
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
mod=sys.argv[1]; pk=sys.argv[2:]   # module dir, package patterns
env=dict(os.environ); env['PATH']=os.path.expanduser('~/go/bin')+':'+env['PATH']
for it in range(60):
    p=subprocess.run(['go','vet']+pk,cwd=mod,env=env,capture_output=True,text=True)
    out=p.stderr
    errs=re.findall(r'^(?:vet: )?(\S+?\.go):(\d+):(\d+): (.*)$',out,re.M)
    if not errs:
        print('clean' if p.returncode==0 else 'FAILED:\n'+out[-1500:]); break
    byfile={}
    imp=[]
    for f,l,c,m in errs:
        f=os.path.join(mod,f) if not os.path.isabs(f) else f
        if 'imported and not used' in m: imp.append((f,int(l))); continue
        if m.startswith('undefined:') or 'has no field or method' in m or 'undeclared name' in m or 'not declared by package' in m or 'declared and not used' in m or 'cannot use' in m or 'too many arguments' in m or 'not enough arguments' in m or 'unknown field' in m:
            byfile.setdefault(f,set()).add(int(l))
        else:
            print('other error',f,l,m)
    if not byfile and not imp: print(out[-1500:]); break
    for f,ls in byfile.items():
        for l in sorted(ls,reverse=True): subprocess.run([S+'/rmdecl',f'{f}:{l}'],capture_output=True)
    for f,l in sorted(imp,key=lambda x:(x[0],-x[1])):
        lines=open(f).read().split('\n'); del lines[l-1]; open(f,'w').write('\n'.join(lines))
    print('iter',it,len(errs),'errors')
