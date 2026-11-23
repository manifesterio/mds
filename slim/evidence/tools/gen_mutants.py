import re,random,json,glob,os,sys
ROOT='/Users/arun.lakshman/workspace/manifester/mds-verify-slim'
random.seed(20261122)
CORE={
 'broker':['broker/internal/broker','broker/internal/core','broker/internal/wire/kafkawire','broker/internal/wire/segmentformat'],
 'controlplane':['controlplane/internal/controlplane/domain/broker','controlplane/internal/controlplane/domain/group','controlplane/internal/controlplane/domain/rebalance','controlplane/internal/controlplane/domain/topic','controlplane/internal/controlplane/replica','controlplane/internal/controlplane/metadb','controlplane/internal/controlplane/executor','controlplane/internal/controlplane/server','controlplane/internal/runtime'],
}
N=int(sys.argv[1])
def blank(s):
    return re.sub(r'"(\\.|[^"\\])*"|`[^`]*`|\'(\\.|[^\'\\])\'', lambda m:'_'*len(m.group(0)), s)
OPS=[(' == ',' != '),(' != ',' == '),(' < ',' <= '),(' <= ',' < '),(' > ',' >= '),(' >= ',' > '),(' && ',' || '),(' || ',' && '),(' + 1',' + 0'),(' - 1',' - 0'),(' += ',' -= '),('return true','return false'),('return false','return true')]
out={}
for mod,dirs in CORE.items():
    cands=[]
    for d in dirs:
        for f in sorted(glob.glob(f'{ROOT}/{d}/*.go')):
            if f.endswith('_test.go') or '_gen' in f or f.endswith('.pb.go'): continue
            rel=os.path.relpath(f,ROOT)
            for i,l in enumerate(open(f).read().split('\n')):
                if not l.startswith('\t'): continue
                st=l.strip()
                if st.startswith('//') or st.startswith('case ') and False: continue
                b=blank(l)
                for a,c in OPS:
                    j=b.find(a)
                    if j>=0: cands.append(dict(file=rel,line=i+1,old=l,new=l[:j]+c+l[j+len(a):],op=a.strip()+'->'+c.strip()))
                m=re.match(r'^(\t+)if (.*[^;]) \{$',l)
                if m and ':=' not in l and ';' not in l: cands.append(dict(file=rel,line=i+1,old=l,new=f'{m.group(1)}if !({m.group(2)}) {{',op='negate-if'))
                if re.match(r'^\t+[A-Za-z_][\w\.]*\(.*\)$',l) and not st.startswith(('defer','go ','return','panic','if','for')) :
                    cands.append(dict(file=rel,line=i+1,old=l,new=re.match(r'^\t+',l).group(0)+'// mutated: statement deleted',op='delete-call'))
    random.shuffle(cands)
    # at most 2 per line already one op per line? keep one op per (file,line)
    seen=set(); pick=[]
    for c in cands:
        k=(c['file'],c['line'])
        if k in seen: continue
        seen.add(k); pick.append(c)
        if len(pick)>=N: break
    print(mod,len(cands),'candidates',len(pick),'picked',file=sys.stderr)
    for n,c in enumerate(pick): c['id']=f'{mod[0]}L{n:02d}'; c['kind']='line'; c['module']=mod
    out[mod]=pick
# bespoke: controlplane mutants.txt
txt=open(f'{ROOT}/ops/controlplane/mutants.txt').read().split('\n')
muts=[];cur=None;mode=None
for l in txt:
    if l.startswith('mutant '): cur=dict(id='cT-'+l.split()[1],kind='text',module='controlplane',op='bespoke',old='',new=''); muts.append(cur);continue
    if cur is None: continue
    if l.startswith('file '): cur['file']='controlplane/'+l[5:].strip(); continue
    if l.startswith('run '): continue
    if l=='@@ old': mode='old'; cur['old']='';continue
    if l=='@@ new': mode='new'; cur['new']='';continue
    if l=='@@ end': mode=None; continue
    if mode: cur[mode]+=l+'\n'
for m in muts: m['old']=m['old'].rstrip('\n')+'\n'; m['new']=m['new'].rstrip('\n')+'\n'
out['controlplane']+=muts
bugs=[('EarlyAck',1),('WrongOffset',2),('ServeCorrupt',3),('NoBatchCap',4),('TimerFromLastBatch',5),('SealOffByOne',6),('NoPartitionCap',7),('FreshKeyOnRetry',8),('SkipProduceCRC',9)]
out['broker']+=[dict(id='bB-'+n,kind='bug',module='broker',bug=v,op='InjectedBug',file='',line=0) for n,v in bugs]
json.dump(out,open(sys.argv[2],'w'),indent=0)
