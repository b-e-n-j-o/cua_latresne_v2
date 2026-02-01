#!/usr/bin/env python3
import psutil
import tracemalloc
import json
from pathlib import Path
from datetime import datetime

# Vos imports existants
from pipeline_from_parcelles import run_pipeline_from_parcelles

class MemoryMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self.snapshots = []
        tracemalloc.start()
        
    def log(self, step_name):
        mem_mb = self.process.memory_info().rss / 1024**2
        current, peak = tracemalloc.get_traced_memory()
        
        snapshot = {
            "step": step_name,
            "timestamp": datetime.now().isoformat(),
            "ram_mb": round(mem_mb, 2),
            "tracemalloc_current_mb": round(current / 1024**2, 2),
            "tracemalloc_peak_mb": round(peak / 1024**2, 2),
        }
        self.snapshots.append(snapshot)
        print(f"[{step_name}] RAM: {mem_mb:.1f}MB | Peak: {peak/1024**2:.1f}MB")
        
    def save_report(self, output_path="memory_diagnostic.json"):
        report = {
            "total_snapshots": len(self.snapshots),
            "peak_ram_mb": max(s["ram_mb"] for s in self.snapshots),
            "final_ram_mb": self.snapshots[-1]["ram_mb"] if self.snapshots else 0,
            "snapshots": self.snapshots
        }
        Path(output_path).write_text(json.dumps(report, indent=2))
        print(f"\n📊 Rapport sauvegardé : {output_path}")
        print(f"🔴 RAM peak : {report['peak_ram_mb']:.1f} MB")
        return report

# Test avec vos données réelles
if __name__ == "__main__":
    monitor = MemoryMonitor()
    
    # Remplacez par vos vraies parcelles de test
    parcelles = [
        {"section": "AC", "numero": "0242"}
    ]
    code_insee = "33234"  # Latresne
    
    monitor.log("START")
    
    try:
        result = run_pipeline_from_parcelles(
            parcelles=parcelles,
            code_insee=code_insee,
            commune_nom="Latresne",
            out_dir="/tmp/test_diagnostic"
        )
        monitor.log("PIPELINE_DONE")
        
    except Exception as e:
        monitor.log(f"ERROR: {e}")
        raise
    finally:
        monitor.save_report()