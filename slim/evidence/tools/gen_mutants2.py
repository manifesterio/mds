import re,random,json,glob,os,sys
ROOT='/Users/arun.lakshman/workspace/manifester/mds-verify-slim'
random.seed(777)
def blank(s):
    return re.sub(r'"(\\.|[^"\\])*"|`[^`]*`|\'(\\.|[^\'\\])\'', lambda m:'_'*len(m.group(0)), s)
OPS=[(' == ',' != '),(' != ',' == '),(' < ',' <= '),(' <= ',' < '),(' > ',' >= '),(' >= ',' > '),(' && ',' || '),(' || ',' && '),(' + 1',' + 0'),(' - 1',' - 0'),(' += ',' -= '),('return true','return false'),('return false','return true')]
GROUPS={
 'infra':(['dst/check/faulty','broker/internal/verify/simulator/fakeservices','broker/internal/verify/simulator/fakeservices/fakenetwork','broker/internal/verify/simulator/cluster'],40),
 'dst':(['dst/kernel','dst/campaign','dst/check/spectest','dst/sched','dst/fault','dst/trace','dst/rng','dst/machine','dst/live','dst/golden'],45),
}
out={}
for g,(dirs,N) in GROUPS.items():
    cands=[]
    for d in dirs:
        for f in sorted(glob.glob(f'{ROOT}/{d}/*.go')):
            if f.endswith('_test.go') or '_gen' in f: continue
            rel=os.path.relpath(f,ROOT)
            for i,l in enumerate(open(f).read().split('\n')):
                if not l.startswith('\t'): continue
                st=l.strip()
                if st.startswith('//'): continue
                b=blank(l)
                for a,c in OPS:
                    j=b.find(a)
                    if j>=0: cands.append(dict(file=rel,line=i+1,old=l,new=l[:j]+c+l[j+len(a):],op=a.strip()+'->'+c.strip()))
                m=re.match(r'^(\t+)if (.*[^;]) \{$',l)
                if m and ':=' not in l and ';' not in l: cands.append(dict(file=rel,line=i+1,old=l,new=f'{m.group(1)}if !({m.group(2)}) {{',op='negate-if'))
                if re.match(r'^\t+[A-Za-z_][\w\.]*\(.*\)$',l) and not st.startswith(('defer','go ','return','panic','if','for')):
                    cands.append(dict(file=rel,line=i+1,old=l,new=re.match(r'^\t+',l).group(0)+'// mutated: statement deleted',op='delete-call'))
    random.shuffle(cands); seen=set(); pick=[]
    for c in cands:
        k=(c['file'],c['line'])
        if k in seen: continue
        seen.add(k); pick.append(c)
        if len(pick)>=N: break
    for n,c in enumerate(pick): c['id']=f'{g}{n:02d}'; c['kind']='line'; c['module']=c['file'].split('/')[0]; c['group']=g
    out[g]=pick; print(g,len(cands),len(pick),file=sys.stderr)
json.dump(out,open(sys.argv[1],'w'))
