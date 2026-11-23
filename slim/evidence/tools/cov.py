import sys,glob,os,collections
S=sys.argv[1]; mod=sys.argv[2]
def load(p):
    d={}
    for i,l in enumerate(open(p)):
        if i==0 or not l.strip(): continue
        a,rest=l.rsplit(' ',2)[0],l.rsplit(' ',2)[1:]
        d[a]=(int(rest[0]),int(rest[1]))
    return d
P={}
for f in sorted(glob.glob(f'{S}/cov/{mod}_*.out')):
    n=os.path.basename(f)[len(mod)+1:-4]
    P[n]=load(f)
allb={}
for n,d in P.items():
    for k,(s,c) in d.items(): allb[k]=s
tot=sum(allb.values())
cov={n:{k for k,(s,c) in d.items() if c>0} for n,d in P.items()}
w=lambda ks: sum(allb[k] for k in ks)
union=set().union(*cov.values())
print(f"module {mod}: core stmts total={tot} covered by any={w(union)} ({100*w(union)/tot:.1f}%)")
print(f"{'layer':58s} {'cov':>6s} {'%core':>6s} {'unique':>6s} {'not-in-unit':>6s}")
for n in sorted(cov,key=lambda n:-w(cov[n])):
    if w(cov[n])==0: continue
    others=set().union(*[c for m,c in cov.items() if m!=n])
    uniq=cov[n]-others
    print(f"{n:58s} {w(cov[n]):6d} {100*w(cov[n])/tot:5.1f}% {w(uniq):6d}")
# groups
groups=eval(open(f'{S}/groups_{mod}.txt').read())
print("\nGROUPS")
gc={g:set().union(*[cov[n] for n in names if n in cov]) if names else set() for g,names in groups.items()}
for g,c in gc.items():
    others=set().union(*[x for h,x in gc.items() if h!=g])
    print(f"{g:30s} cov={w(c):6d} ({100*w(c)/tot:5.1f}%) unique={w(c-others):5d}  covered-by-others-only={w(c&others)}")
# unique by file per group
import re
for g,c in gc.items():
    others=set().union(*[x for h,x in gc.items() if h!=g])
    u=c-others
    byf=collections.Counter()
    for k in u: byf[k.split(':')[0].split('/mds/')[-1]]+=allb[k]
    print(f"\nunique-to {g}: {w(u)} stmts")
    for f,n in byf.most_common(12): print(f"   {n:5d} {f}")
