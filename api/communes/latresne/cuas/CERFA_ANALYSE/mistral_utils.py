"""
Utils pour analyses LLM avec Mistral AI
"""

import os
import base64
from pathlib import Path
from typing import Optional, List
from mistralai import Mistral
from pdf2image import convert_from_path


class MistralAnalyzer:
    """Analyseur LLM avec différentes méthodes d'input"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            raise ValueError("MISTRAL_API_KEY requise")
        self.client = Mistral(api_key=self.api_key)
    
    def analyze_text(
        self,
        prompt: str,
        model: str = "ministral-8b-2512",
        max_tokens: int = 4096,
        temperature: float = 0.0
    ) -> dict:
        """
        Analyse texte simple
        
        Args:
            prompt: Question/instruction
            model: Modèle Mistral
            max_tokens: Limite tokens réponse
            temperature: 0.0 = déterministe, 1.0 = créatif
            
        Returns:
            {"content": str, "tokens": int, "model": str}
        """
        response = self.client.chat.complete(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens,
            "model": response.model
        }
    
    def analyze_pdf_ocr(
        self,
        pdf_path: str,
        prompt: str,
        model: str = "ministral-8b-2512",
        max_tokens: int = 4096,
        temperature: float = 0.0
    ) -> dict:
        """
        Analyse PDF via OCR (texte brut)
        
        Args:
            pdf_path: Chemin PDF
            prompt: Question/instruction
            model: Modèle Mistral
            
        Returns:
            {"content": str, "tokens": int, "model": str, "ocr_pages": int}
        """
        # Upload
        uploaded = self.client.files.upload(
            file={"file_name": Path(pdf_path).name, "content": open(pdf_path, "rb")},
            purpose="ocr"
        )
        url = self.client.files.get_signed_url(file_id=uploaded.id).url
        
        # OCR
        ocr_response = self.client.ocr.process(
            model="mistral-ocr-latest",
            document={"type": "document_url", "document_url": url},
            include_image_base64=False
        )
        
        # Extraire texte
        full_text = "\n\n".join([page.markdown for page in ocr_response.pages])
        
        # Analyse LLM
        combined_prompt = f"{prompt}\n\n=== DOCUMENT ===\n{full_text}"
        
        response = self.client.chat.complete(
            model=model,
            messages=[{"role": "user", "content": combined_prompt}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        # Cleanup
        self.client.files.delete(file_id=uploaded.id)
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens,
            "model": response.model,
            "ocr_pages": len(ocr_response.pages)
        }
    
    def analyze_pdf_vision(
        self,
        pdf_path: str,
        prompt: str,
        model: str = "ministral-14b-2512",
        pages: Optional[List[int]] = None,
        dpi: int = 300,
        max_tokens: int = 4096,
        temperature: float = 0.0
    ) -> dict:
        """
        Analyse PDF via Vision (images)
        
        Args:
            pdf_path: Chemin PDF
            prompt: Question/instruction
            model: Modèle Mistral (14b recommandé)
            pages: Pages à analyser (ex: [1,2,4]) ou None pour toutes
            dpi: Résolution images
            
        Returns:
            {"content": str, "tokens": int, "model": str, "images_processed": int}
        """
        # Déterminer pages
        if pages is None:
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            pages = list(range(1, len(reader.pages) + 1))
        
        # Convertir en images
        images_b64 = []
        for page_num in pages:
            imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_num, last_page=page_num)
            tmp = f"/tmp/page_{page_num}.png"
            imgs[0].save(tmp, "PNG")
            
            with open(tmp, "rb") as f:
                images_b64.append(base64.b64encode(f.read()).decode())
            os.remove(tmp)
        
        # Construire message
        content = [{"type": "text", "text": prompt}]
        for img_b64 in images_b64:
            content.append({
                "type": "image_url",
                "image_url": f"data:image/png;base64,{img_b64}"
            })
        
        # Analyse
        response = self.client.chat.complete(
            model=model,
            messages=[{"role": "user", "content": content}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens,
            "model": response.model,
            "images_processed": len(images_b64)
        }


# Fonctions helpers
def analyze_text(prompt: str, model: str = "ministral-8b-2512", api_key: Optional[str] = None) -> dict:
    """Analyse texte simple"""
    analyzer = MistralAnalyzer(api_key=api_key)
    return analyzer.analyze_text(prompt, model=model)


def analyze_pdf_ocr(pdf_path: str, prompt: str, model: str = "ministral-8b-2512", api_key: Optional[str] = None) -> dict:
    """Analyse PDF via OCR"""
    analyzer = MistralAnalyzer(api_key=api_key)
    return analyzer.analyze_pdf_ocr(pdf_path, prompt, model=model)


def analyze_pdf_vision(pdf_path: str, prompt: str, pages: Optional[List[int]] = None, model: str = "ministral-14b-2512", api_key: Optional[str] = None) -> dict:
    """Analyse PDF via Vision (images)"""
    analyzer = MistralAnalyzer(api_key=api_key)
    return analyzer.analyze_pdf_vision(pdf_path, prompt, pages=pages, model=model)


# Exemples
if __name__ == "__main__":
    
    # 1. Analyse texte
    result = analyze_text(
        "Explique la différence entre OCR et Vision en 2 phrases",
        model="ministral-3b-2512"
    )
    print("TEXTE:", result["content"])
    
    # 2. Analyse PDF via OCR
    result = analyze_pdf_ocr(
        pdf_path="document.pdf",
        prompt="Résume ce document",
        model="ministral-8b-2512"
    )
    print("OCR:", result["content"])
    
    # 3. Analyse PDF via Vision
    result = analyze_pdf_vision(
        pdf_path="document.pdf",
        prompt="Extrais les tableaux",
        pages=[2, 4],
        model="ministral-14b-2512"
    )
    print("VISION:", result["content"])