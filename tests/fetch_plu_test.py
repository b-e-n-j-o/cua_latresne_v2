# tests/test_plu_cache.py
import requests
import time

API_BASE = "http://localhost:8000"
INSEE = "33234"  # Latresne

def test_first_download():
    """Premier téléchargement (sans cache)"""
    print(f"\n📥 PREMIER APPEL - Téléchargement depuis GPU")
    print(f"   INSEE: {INSEE}")
    print("   " + "="*60)
    
    start_time = time.perf_counter()
    response = requests.get(f"{API_BASE}/api/plu/reglement/{INSEE}", stream=True)
    ttfb = time.perf_counter() - start_time
    
    print(f"   ⏱️  TTFB: {ttfb:.2f}s")
    print(f"   📡 Status: {response.status_code}")
    print(f"   📄 Content-Type: {response.headers.get('content-type')}")
    
    assert response.status_code == 200
    
    # Déterminer si JSON (URL signée) ou PDF direct
    content_type = response.headers.get('content-type')
    
    if 'application/json' in content_type:
        data = response.json()
        print(f"   🔗 Type réponse: URL signée Supabase")
        print(f"   💾 Mis en cache: {'Oui' if not data.get('cached') else 'Non (déjà présent)'}")
        print(f"   🌐 URL: {data['url'][:70]}...")
        
        # Télécharger depuis l'URL signée pour mesurer
        dl_start = time.perf_counter()
        pdf_response = requests.get(data['url'], stream=True)
        total_size = int(pdf_response.headers.get('content-length', 0))
        
        downloaded = 0
        chunks = []
        for chunk in pdf_response.iter_content(8192):
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
        
        dl_time = time.perf_counter() - dl_start
        content = b''.join(chunks)
    else:
        print(f"   📦 Type réponse: PDF direct (trop gros pour cache)")
        total_size = int(response.headers.get('content-length', 0))
        
        downloaded = 0
        chunks = []
        print("   📊 Progression: [", end="", flush=True)
        
        for chunk in response.iter_content(8192):
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = (downloaded / total_size) * 100
                    bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
                    print(f"\r   📊 Progression: [{bar}] {pct:.1f}%", end="", flush=True)
        
        print()
        dl_time = time.perf_counter() - start_time
        content = b''.join(chunks)
    
    total_time = time.perf_counter() - start_time
    
    print(f"   📊 Taille: {len(content) / 1024:.1f} Ko ({len(content) / (1024*1024):.2f} Mo)")
    print(f"   ⏱️  Temps total: {total_time:.2f}s")
    print(f"   🚀 Vitesse: {len(content) / total_time / 1024:.1f} Ko/s")
    
    with open(f"test_reglement_{INSEE}_first.pdf", "wb") as f:
        f.write(content)
    print(f"   ✅ Sauvegardé: test_reglement_{INSEE}_first.pdf")
    
    return total_time, len(content)

def test_cached_download():
    """Second téléchargement (depuis cache)"""
    print(f"\n📦 SECOND APPEL - Depuis cache Supabase")
    print("   " + "="*60)
    
    start_time = time.perf_counter()
    response = requests.get(f"{API_BASE}/api/plu/reglement/{INSEE}")
    ttfb = time.perf_counter() - start_time
    
    print(f"   ⏱️  TTFB: {ttfb:.2f}s")
    print(f"   📡 Status: {response.status_code}")
    
    assert response.status_code == 200
    
    content_type = response.headers.get('content-type')
    
    if 'application/json' in content_type:
        data = response.json()
        print(f"   💾 Cached: {data.get('cached')}")
        assert data.get('cached') == True, "❌ Devrait être en cache!"
        print(f"   🌐 URL: {data['url'][:70]}...")
        
        # Télécharger depuis cache
        dl_start = time.perf_counter()
        pdf_response = requests.get(data['url'])
        content = pdf_response.content
        dl_time = time.perf_counter() - dl_start
    else:
        content = response.content
        dl_time = time.perf_counter() - start_time
    
    total_time = time.perf_counter() - start_time
    
    print(f"   📊 Taille: {len(content) / 1024:.1f} Ko")
    print(f"   ⏱️  Temps total: {total_time:.2f}s")
    print(f"   🚀 Vitesse: {len(content) / total_time / 1024:.1f} Ko/s")
    print(f"   ✅ Cache vérifié!")
    
    return total_time, len(content)

def test_plu_cache():
    print("\n" + "="*70)
    print("🧪 TEST SYSTÈME DE CACHE PLU")
    print("="*70)
    
    t1, size1 = test_first_download()
    time.sleep(1)  # Pause courte
    t2, size2 = test_cached_download()
    
    print("\n" + "="*70)
    print("📊 COMPARAISON")
    print("="*70)
    print(f"   Premier appel:  {t1:.2f}s")
    print(f"   Second appel:   {t2:.2f}s")
    print(f"   ⚡ Gain:         {t1/t2:.1f}x plus rapide")
    print(f"   📦 Tailles:      {size1 == size2} (identiques: {size1 == size2})")
    print("\n✅ Tous les tests passés!")

if __name__ == "__main__":
    test_plu_cache()