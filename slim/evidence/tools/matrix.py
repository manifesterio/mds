import json,glob,sys,collections,itertools
S='/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim'
pat=sys.argv[1]; prefix=sys.argv[2] if len(sys.argv)>2 else ''
R=[json.loads(l) for f in sorted(glob.glob(S+'/'+pat)) for l in open(f)]
R=[r for r in R if r['id'].startswith(prefix)]
ok=[r for r in R if r['status']=='ok']
print(f'{len(R)} mutants, {len(R)-len(ok)} did not compile, {len(ok)} evaluated')
layers=list(ok[0]['layers'].keys()) if ok else []
for r in ok: 
    for k,v in r['layers'].items():
        if v['buildfail'] or v['timeout']: print('WARN',r['id'],k,'buildfail/timeout')
K=lambda r,l: r['layers'][l]['killed']
killed_any=[r for r in ok if any(K(r,l) for l in layers)]
print(f'killed by at least one layer: {len(killed_any)} ; survived all: {len(ok)-len(killed_any)}')
print(f"{'layer':26s} kills  unique-kills  (of {len(killed_any)} killed)")
for l in layers:
    ks=[r for r in killed_any if K(r,l)]
    un=[r for r in ks if all(not K(r,m) for m in layers if m!=l)]
    print(f'{l:26s} {len(ks):4d}   {len(un):4d}  {[r["id"] for r in un]}')
print('\nkill-set patterns:')
pc=collections.Counter(tuple(l for l in layers if K(r,l)) for r in ok)
for p,c in pc.most_common(): print(f'  {c:3d}  {p if p else "SURVIVED"}')
# per-package kills in unit layer
for l in layers:
    pk=collections.Counter(f for r in ok if K(r,l) for f in r['layers'][l]['fails'])
    un=collections.Counter()
    for r in ok:
        if K(r,l):
            fs=r['layers'][l]['fails']
            if len(fs)==1 and all(not K(r,m) for m in layers if m!=l): un[fs[0]]+=1
    if len(pk)>1: print(f'\n{l}: kills by package {dict(pk)}; sole killer overall: {dict(un)}')
print('\nsurvivors:')
for r in ok:
    if not any(K(r,l) for l in layers): print('  ',r['id'],r['op'],r['file'],r['line'])
