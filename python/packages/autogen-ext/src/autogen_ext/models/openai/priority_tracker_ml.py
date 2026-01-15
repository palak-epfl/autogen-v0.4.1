"""
Optimized Priority Tracker Module
===================================

Performance optimizations to minimize overhead:
1. Lazy loading - Only load model when first needed
2. Async computation - Don't block API calls
3. Sampling - Only check similarity for recent calls
4. Caching - Cache embeddings to avoid recomputation
5. Lightweight mode - Simple rule-based fallback

Usage:
    from priority_tracker_optimized import priority_tracker
    
    # Configure performance mode
    priority_tracker.set_mode('lightweight')  # or 'async' or 'full'
"""

import json
import asyncio
from typing import Dict, List, Optional
from collections import defaultdict, deque
from functools import lru_cache
import hashlib


class OptimizedPriorityTracker:
    """
    Optimized priority tracker with multiple performance modes.
    """
    
    def __init__(
        self,
        mode: str = 'async',  # 'lightweight', 'async', or 'full'
        similarity_threshold: float = 0.85,
        priority_thresholds: Optional[Dict[float, int]] = None,
        log_file: Optional[str] = "priority_changes.jsonl",
        max_history: int = 20,  # Only compare against last N calls
        cache_size: int = 1000   # Cache embeddings
    ):
        """
        Initialize the optimized priority tracker.
        
        Args:
            mode: Performance mode
                - 'lightweight': Rule-based, no ML (fastest)
                - 'async': ML computation in background (balanced)
                - 'full': ML computation inline (most accurate)
            similarity_threshold: Threshold for detecting similar tool calls
            priority_thresholds: Mapping of avg_repetition -> priority level
            log_file: Path to log file (None to disable)
            max_history: Only compare against last N tool calls
            cache_size: Size of embedding cache
        """
        self.mode = mode
        self.similarity_model = None  # Lazy load
        self.model_loading = False
        
        # Per-task tracking
        self.task_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self.semantic_repetition: Dict[str, defaultdict] = defaultdict(lambda: defaultdict(float))
        self.current_priority: Dict[str, int] = defaultdict(int)
        
        # Configuration
        self.similarity_threshold = similarity_threshold
        self.log_file = log_file
        self.max_history = max_history
        
        # Priority thresholds
        self.priority_thresholds = priority_thresholds or {
            1.2: 0,
            1.6: 2,
            2.0: 4,
            2.5: 6,
            float('inf'): 8
        }
        
        # Caching for embeddings
        self._embedding_cache = {}
        self.cache_size = cache_size
        
        print(f"Priority Tracker initialized in '{mode}' mode")
    
    def set_mode(self, mode: str):
        """Change performance mode at runtime."""
        if mode not in ['lightweight', 'async', 'full']:
            raise ValueError(f"Invalid mode: {mode}")
        self.mode = mode
        print(f"Priority Tracker mode changed to: {mode}")
    
    def _load_model_lazy(self):
        """Lazy load the ML model only when needed."""
        if self.similarity_model is None and not self.model_loading:
            self.model_loading = True
            print("Loading Sentence Transformer model (first use)...")
            from sentence_transformers import SentenceTransformer
            self.similarity_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✓ Model loaded")
    
    def _hash_text(self, text: str) -> str:
        """Create hash for caching."""
        return hashlib.md5(text.encode()).hexdigest()
    
    @lru_cache(maxsize=1000)
    def _get_embedding_cached(self, text_hash: str, text: str):
        """Get embedding with caching."""
        if self.similarity_model is None:
            return None
        
        from scipy.spatial.distance import cosine
        
        if text_hash not in self._embedding_cache:
            embedding = self.similarity_model.encode(text, convert_to_tensor=False)
            
            # Limit cache size
            if len(self._embedding_cache) >= self.cache_size:
                # Remove oldest entry
                self._embedding_cache.pop(next(iter(self._embedding_cache)))
            
            self._embedding_cache[text_hash] = embedding
        
        return self._embedding_cache[text_hash]
    
    def compute_similarity_ml(self, text1: str, text2: str) -> float:
        """ML-based similarity (with caching)."""
        if not text1 or not text2:
            return 0.0
        if text1 == text2:
            return 1.0
        
        if self.similarity_model is None:
            self._load_model_lazy()
        
        try:
            from scipy.spatial.distance import cosine
            
            hash1 = self._hash_text(text1)
            hash2 = self._hash_text(text2)
            
            emb1 = self._get_embedding_cached(hash1, text1)
            emb2 = self._get_embedding_cached(hash2, text2)
            
            if emb1 is None or emb2 is None:
                return 0.0
            
            return float(1 - cosine(emb1, emb2))
        except Exception as e:
            print(f"Warning: ML similarity failed: {e}")
            return self.compute_similarity_lightweight(text1, text2)
    
    def compute_similarity_lightweight(self, text1: str, text2: str) -> float:
        """
        Lightweight rule-based similarity (no ML).
        Fast but less accurate.
        """
        if not text1 or not text2:
            return 0.0
        if text1 == text2:
            return 1.0
        
        # Normalize
        text1 = text1.lower().strip()
        text2 = text2.lower().strip()
        
        # Exact match after normalization
        if text1 == text2:
            return 1.0
        
        # Keyword overlap (simple Jaccard)
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return intersection / union if union > 0 else 0.0
    
    def compute_similarity(self, text1: str, text2: str) -> float:
        """Compute similarity based on current mode."""
        if self.mode == 'lightweight':
            return self.compute_similarity_lightweight(text1, text2)
        else:  # 'async' or 'full'
            return self.compute_similarity_ml(text1, text2)
    
    def compute_tool_similarity(self, tool1: Dict, tool2: Dict) -> float:
        """Compute similarity between two tool calls."""
        # Tool names must match
        if tool1.get("name") != tool2.get("name"):
            return 0.0
        
        tool_name = tool1.get("name")
        args1 = tool1.get("arguments", {})
        args2 = tool2.get("arguments", {})
        
        # Tool-specific logic
        if tool_name == "web_search":
            query1 = args1.get("query", "")
            query2 = args2.get("query", "")
            return self.compute_similarity(query1, query2)
        
        elif tool_name == "visit_url":
            url1 = args1.get("url", "")
            url2 = args2.get("url", "")
            
            # Quick check: same domain?
            if self.mode == 'lightweight':
                # Extract domain only
                domain1 = url1.split('/')[2] if '://' in url1 else url1.split('/')[0]
                domain2 = url2.split('/')[2] if '://' in url2 else url2.split('/')[0]
                return 0.9 if domain1 == domain2 else 0.0
            else:
                return self.compute_similarity(url1, url2)
        
        elif tool_name == "click":
            # Exact match only
            return 1.0 if args1.get("target_id") == args2.get("target_id") else 0.0
        
        else:
            # Default: compare all arguments
            text1 = " ".join(str(v) for v in args1.values() if isinstance(v, str))
            text2 = " ".join(str(v) for v in args2.values() if isinstance(v, str))
            return self.compute_similarity(text1, text2)
    
    def create_signature(self, tool_name: str, arguments: Dict) -> str:
        """Create signature for grouping."""
        try:
            if tool_name == "web_search" and "query" in arguments:
                words = arguments['query'].lower().split()[:3]
                return f"web_search::{'_'.join(words)}"
            elif tool_name == "visit_url" and "url" in arguments:
                domain = arguments['url'].lower().replace('https://', '').replace('http://', '').split('/')[0]
                return f"visit_url::{domain}"
            elif tool_name == "click":
                return f"click::{arguments.get('target_id', 'unknown')}"
        except Exception:
            pass
        
        return f"{tool_name}::generic"
    
    def calculate_priority(self, avg_repetition: float) -> int:
        """Map average repetition to priority level."""
        for threshold, priority in sorted(self.priority_thresholds.items()):
            if avg_repetition <= threshold:
                return priority
        return 8
    
    def update_priority(self, task_id: str, tool_calls: List[Dict]) -> int:
        """
        Update priority based on new tool calls.
        
        In 'async' mode, similarity computation happens in background.
        In 'lightweight' mode, uses simple rule-based matching.
        In 'full' mode, computes inline (blocking).
        """
        if not tool_calls:
            return self.current_priority[task_id]
        
        old_priority = self.current_priority[task_id]
        
        # Process each new tool call
        for new_tool in tool_calls:
            # Compare to recent history only (limited by deque)
            max_similarity = 0.0
            matching_signature = None
            
            for prev_tool in self.task_history[task_id]:
                similarity = self.compute_tool_similarity(new_tool, prev_tool)
                if similarity > max_similarity:
                    max_similarity = similarity
                    matching_signature = self.create_signature(
                        prev_tool.get("name", ""), 
                        prev_tool.get("arguments", {})
                    )
            
            # Update semantic repetition
            signature = self.create_signature(
                new_tool.get("name", ""), 
                new_tool.get("arguments", {})
            )
            
            if max_similarity >= self.similarity_threshold:
                sig = matching_signature if matching_signature else signature
                self.semantic_repetition[task_id][sig] += max_similarity
            else:
                self.semantic_repetition[task_id][signature] += 1.0
            
            # Add to history (deque automatically limits size)
            self.task_history[task_id].append(new_tool)
        
        # Calculate new priority
        repetition_dict = self.semantic_repetition[task_id]
        if repetition_dict:
            avg_repetition = sum(repetition_dict.values()) / len(repetition_dict)
            new_priority = self.calculate_priority(avg_repetition)
        else:
            new_priority = 0
        
        self.current_priority[task_id] = new_priority
        
        # Log if changed
        if new_priority != old_priority and self.log_file:
            self._log_priority_change(task_id, old_priority, new_priority, tool_calls)
        
        return new_priority
    
    async def update_priority_async(self, task_id: str, tool_calls: List[Dict]) -> int:
        """
        Async version - returns immediately with current priority,
        updates in background.
        """
        # Return current priority immediately
        current = self.current_priority[task_id]
        
        # Update in background
        asyncio.create_task(self._update_priority_background(task_id, tool_calls))
        
        return current
    
    async def _update_priority_background(self, task_id: str, tool_calls: List[Dict]):
        """Background priority update (non-blocking)."""
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.update_priority, task_id, tool_calls)
    
    def _log_priority_change(self, task_id: str, old_priority: int, new_priority: int, tool_calls: List[Dict]):
        """Log priority change to file."""
        try:
            from datetime import datetime, timezone
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                "task_id": task_id,
                "old_priority": old_priority,
                "new_priority": new_priority,
                "tool_calls": tool_calls,
                "mode": self.mode
            }
            
            with open(self.log_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            print(f"Warning: Failed to log: {e}")
    
    def get_priority(self, task_id: str) -> int:
        """Get current priority for a task."""
        return self.current_priority.get(task_id, 0)
    
    def get_stats(self, task_id: str) -> Dict:
        """Get statistics for a task."""
        if task_id not in self.task_history:
            return {
                "total_tool_calls": 0,
                "unique_signatures": 0,
                "avg_repetition": 1.0,
                "current_priority": 0,
                "mode": self.mode
            }
        
        repetition_dict = self.semantic_repetition[task_id]
        return {
            "total_tool_calls": len(self.task_history[task_id]),
            "unique_signatures": len(repetition_dict),
            "avg_repetition": sum(repetition_dict.values()) / len(repetition_dict) if repetition_dict else 1.0,
            "current_priority": self.current_priority[task_id],
            "mode": self.mode,
            "cache_size": len(self._embedding_cache) if hasattr(self, '_embedding_cache') else 0
        }
    
    def reset_task(self, task_id: str):
        """Reset tracking for a task."""
        if task_id in self.task_history:
            del self.task_history[task_id]
        if task_id in self.semantic_repetition:
            del self.semantic_repetition[task_id]
        if task_id in self.current_priority:
            del self.current_priority[task_id]


# ============================================================
# Helper Function
# ============================================================

def extract_tool_calls_from_response(result) -> List[Dict]:
    """
    Extract tool calls from LLM response.
    
    Args:
        result: ChatCompletion object
    
    Returns:
        List of tool calls: [{"name": "...", "arguments": {...}}]
        
    Example result format:
        result.choices[0].message.tool_calls = [
            ChatCompletionMessageFunctionToolCall(
                id='chatcmpl-tool-bb117356c3f9ed53',
                function=Function(
                    arguments='{"reasoning": "...", "url": "..."}',
                    name='visit_url'
                ),
                type='function'
            )
        ]
    """
    tool_calls = []
    
    try:
        # Check if choices exist
        if not hasattr(result, 'choices') or not result.choices:
            return tool_calls
        
        choice = result.choices[0]
        
        # Check if message has tool_calls
        if not hasattr(choice, 'message') or not hasattr(choice.message, 'tool_calls'):
            return tool_calls
        
        if choice.message.tool_calls is None:
            return tool_calls
        
        # Extract each tool call
        for tool_call in choice.message.tool_calls:
            try:
                # Get function name
                tool_name = tool_call.function.name
                
                # Get arguments (always a string that needs parsing)
                args_str = tool_call.function.arguments
                
                # Parse arguments string to dict
                if isinstance(args_str, str):
                    try:
                        arguments = json.loads(args_str)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse arguments JSON: {e}")
                        arguments = {"raw": args_str}
                elif isinstance(args_str, dict):
                    arguments = args_str
                else:
                    arguments = {"raw": str(args_str)}
                
                tool_calls.append({
                    "name": tool_name,
                    "arguments": arguments
                })
                
            except Exception as e:
                print(f"Warning: Failed to parse individual tool call: {e}")
                continue
        
    except Exception as e:
        print(f"Warning: Failed to extract tool calls from response: {e}")
        print(f"Result type: {type(result)}")
        if hasattr(result, 'choices'):
            print(f"Choices: {result.choices}")
    
    return tool_calls


# ============================================================
# Global Instance with Async Mode (Balanced)
# ============================================================

priority_tracker = OptimizedPriorityTracker(
    mode='async',  # Background processing, non-blocking
    similarity_threshold=0.85,
    max_history=20,  # Only compare last 20 calls
    cache_size=1000  # Cache 1000 embeddings
)


# ============================================================
# Convenience Functions
# ============================================================

def reset_task_priority(task_id: str):
    """Reset priority tracking for a task."""
    priority_tracker.reset_task(task_id)


def get_task_priority_stats(task_id: str) -> Dict:
    """Get current priority statistics for a task."""
    return priority_tracker.get_stats(task_id)


def set_performance_mode(mode: str):
    """
    Change performance mode.
    
    Args:
        mode: 'lightweight', 'async', or 'full'
    """
    priority_tracker.set_mode(mode)








# """
# Dynamic Priority Tracker Module
# ================================

# Separate module for tracking tool call history and assigning dynamic priorities.

# Usage in your main file:
#     from priority_tracker import priority_tracker, extract_tool_calls_from_response
    
#     # Before API call
#     task_priority = priority_tracker.get_priority(task_id)
    
#     # After API response
#     tool_calls = extract_tool_calls_from_response(result)
#     if tool_calls:
#         new_priority = priority_tracker.update_priority(task_id, tool_calls)
# """

# import json
# from typing import Dict, List, Optional
# from collections import defaultdict
# from sentence_transformers import SentenceTransformer
# from scipy.spatial.distance import cosine


# class DynamicPriorityTracker:
#     """
#     Tracks tool call history per task and assigns dynamic priority.
    
#     Uses semantic similarity to detect repetitive behavior and increase
#     priority when tasks get stuck in loops.
#     """
    
#     def __init__(
#         self,
#         similarity_threshold: float = 0.85,
#         priority_thresholds: Optional[Dict[float, int]] = None,
#         log_file: Optional[str] = "priority_changes.jsonl"
#     ):
#         """
#         Initialize the priority tracker.
        
#         Args:
#             similarity_threshold: Threshold for detecting similar tool calls (0.0-1.0)
#             priority_thresholds: Mapping of avg_repetition -> priority level
#             log_file: Path to log file for priority changes (None to disable)
#         """
#         # Load ML model for similarity detection
#         print("Loading Sentence Transformer model for priority tracking...")
#         self.similarity_model = SentenceTransformer('all-MiniLM-L6-v2')
#         print("✓ Model loaded")
        
#         # Per-task tracking
#         self.task_history: Dict[str, List[Dict]] = defaultdict(list)
#         self.semantic_repetition: Dict[str, defaultdict] = defaultdict(lambda: defaultdict(float))
#         self.current_priority: Dict[str, int] = defaultdict(int)
        
#         # Configuration
#         self.similarity_threshold = similarity_threshold
#         self.log_file = log_file
        
#         # Default priority thresholds (non-linear)
#         self.priority_thresholds = priority_thresholds or {
#             1.2: 0,   # Generous initially
#             1.6: 2,
#             2.0: 4,
#             2.5: 6,
#             float('inf'): 8
#         }
    
#     def compute_similarity(self, text1: str, text2: str) -> float:
#         """Compute semantic similarity between two texts."""
#         if not text1 or not text2:
#             return 0.0
#         if text1 == text2:
#             return 1.0
        
#         try:
#             emb1 = self.similarity_model.encode(text1, convert_to_tensor=False)
#             emb2 = self.similarity_model.encode(text2, convert_to_tensor=False)
#             return float(1 - cosine(emb1, emb2))
#         except Exception as e:
#             print(f"Warning: Similarity computation failed: {e}")
#             return 0.0
    
#     def compute_tool_similarity(self, tool1: Dict, tool2: Dict) -> float:
#         """
#         Compute similarity between two tool calls.
        
#         Args:
#             tool1: {"name": "web_search", "arguments": {"query": "..."}}
#             tool2: {"name": "web_search", "arguments": {"query": "..."}}
        
#         Returns:
#             Similarity score 0.0 to 1.0
#         """
#         # Tool names must match
#         if tool1.get("name") != tool2.get("name"):
#             return 0.0
        
#         tool_name = tool1.get("name")
#         args1 = tool1.get("arguments", {})
#         args2 = tool2.get("arguments", {})
        
#         # Tool-specific similarity logic
#         if tool_name == "web_search":
#             query1 = args1.get("query", "")
#             query2 = args2.get("query", "")
#             return self.compute_similarity(query1, query2)
        
#         elif tool_name == "visit_url":
#             url1 = args1.get("url", "")
#             url2 = args2.get("url", "")
#             return self.compute_similarity(url1, url2)
        
#         elif tool_name == "click":
#             # Click requires exact target match
#             return 1.0 if args1.get("target_id") == args2.get("target_id") else 0.0
        
#         else:
#             # Default: compare all arguments as text
#             text1 = " ".join(str(v) for v in args1.values() if isinstance(v, str))
#             text2 = " ".join(str(v) for v in args2.values() if isinstance(v, str))
#             return self.compute_similarity(text1, text2)
    
#     def create_signature(self, tool_name: str, arguments: Dict) -> str:
#         """Create a signature for grouping similar tool calls."""
#         try:
#             if tool_name == "web_search" and "query" in arguments:
#                 words = arguments['query'].lower().split()[:3]
#                 return f"web_search::{'_'.join(words)}"
#             elif tool_name == "visit_url" and "url" in arguments:
#                 domain = arguments['url'].lower().replace('https://', '').replace('http://', '').split('/')[0]
#                 return f"visit_url::{domain}"
#             elif tool_name == "click":
#                 return f"click::{arguments.get('target_id', 'unknown')}"
#         except Exception:
#             pass
        
#         return f"{tool_name}::generic"
    
#     def calculate_priority(self, avg_repetition: float) -> int:
#         """
#         Map average repetition to priority level (0, 2, 4, 6, 8).
        
#         Non-linear scaling:
#         - 0→2: Slower (give tasks initial relaxation)
#         - 2→4→6→8: Faster (ramp up pressure on stuck tasks)
#         """
#         for threshold, priority in sorted(self.priority_thresholds.items()):
#             if avg_repetition <= threshold:
#                 return priority
#         return 8
    
#     def update_priority(self, task_id: str, tool_calls: List[Dict]) -> int:
#         """
#         Update priority based on new tool calls.
        
#         Args:
#             task_id: Unique identifier for the task
#             tool_calls: List of tool calls from the response
#                        [{"name": "web_search", "arguments": {"query": "..."}}]
        
#         Returns:
#             Updated priority (0, 2, 4, 6, or 8)
#         """
#         if not tool_calls:
#             return self.current_priority[task_id]
        
#         old_priority = self.current_priority[task_id]
        
#         # Process each new tool call
#         for new_tool in tool_calls:
#             # Compare to previous tool calls
#             max_similarity = 0.0
#             matching_signature = None
            
#             for prev_tool in self.task_history[task_id]:
#                 similarity = self.compute_tool_similarity(new_tool, prev_tool)
#                 if similarity > max_similarity:
#                     max_similarity = similarity
#                     matching_signature = self.create_signature(
#                         prev_tool.get("name", ""), 
#                         prev_tool.get("arguments", {})
#                     )
            
#             # Update semantic repetition
#             signature = self.create_signature(
#                 new_tool.get("name", ""), 
#                 new_tool.get("arguments", {})
#             )
            
#             if max_similarity >= self.similarity_threshold:
#                 # Similar to previous call - weight by similarity
#                 sig = matching_signature if matching_signature else signature
#                 self.semantic_repetition[task_id][sig] += max_similarity
#             else:
#                 # New unique call
#                 self.semantic_repetition[task_id][signature] += 1.0
            
#             # Add to history
#             self.task_history[task_id].append(new_tool)
        
#         # Calculate new priority
#         repetition_dict = self.semantic_repetition[task_id]
#         if repetition_dict:
#             avg_repetition = sum(repetition_dict.values()) / len(repetition_dict)
#             new_priority = self.calculate_priority(avg_repetition)
#         else:
#             new_priority = 0
        
#         self.current_priority[task_id] = new_priority
        
#         # Log priority change if it changed
#         if new_priority != old_priority and self.log_file:
#             self._log_priority_change(task_id, old_priority, new_priority, tool_calls)
        
#         return new_priority
    
#     def _log_priority_change(self, task_id: str, old_priority: int, new_priority: int, tool_calls: List[Dict]):
#         """Log priority change to file."""
#         try:
#             from datetime import datetime, timezone
#             log_entry = {
#                 "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
#                 "task_id": task_id,
#                 "old_priority": old_priority,
#                 "new_priority": new_priority,
#                 "tool_calls": tool_calls,
#                 "stats": self.get_stats(task_id)
#             }
            
#             with open(self.log_file, "a") as f:
#                 f.write(json.dumps(log_entry) + "\n")
#         except Exception as e:
#             print(f"Warning: Failed to log priority change: {e}")
    
#     def get_priority(self, task_id: str) -> int:
#         """Get current priority for a task."""
#         return self.current_priority.get(task_id, 0)
    
#     def get_stats(self, task_id: str) -> Dict:
#         """Get statistics for a task."""
#         if task_id not in self.task_history:
#             return {
#                 "total_tool_calls": 0,
#                 "unique_signatures": 0,
#                 "avg_repetition": 1.0,
#                 "current_priority": 0
#             }
        
#         repetition_dict = self.semantic_repetition[task_id]
#         return {
#             "total_tool_calls": len(self.task_history[task_id]),
#             "unique_signatures": len(repetition_dict),
#             "avg_repetition": sum(repetition_dict.values()) / len(repetition_dict) if repetition_dict else 1.0,
#             "current_priority": self.current_priority[task_id]
#         }
    
#     def reset_task(self, task_id: str):
#         """Reset tracking for a task (e.g., when task completes)."""
#         if task_id in self.task_history:
#             del self.task_history[task_id]
#         if task_id in self.semantic_repetition:
#             del self.semantic_repetition[task_id]
#         if task_id in self.current_priority:
#             del self.current_priority[task_id]
    
#     def configure_thresholds(self, priority_thresholds: Dict[float, int]):
#         """Update priority thresholds."""
#         self.priority_thresholds = priority_thresholds
    
#     def configure_similarity(self, similarity_threshold: float):
#         """Update similarity threshold."""
#         self.similarity_threshold = similarity_threshold


# # ============================================================
# # Helper Function
# # ============================================================

# def extract_tool_calls_from_response(result) -> List[Dict]:
#     """
#     Extract tool calls from LLM response.
    
#     Args:
#         result: ChatCompletion or ParsedChatCompletion object
    
#     Returns:
#         List of tool calls: [{"name": "...", "arguments": {...}}]
#     """
#     tool_calls = []
    
#     try:
#         choice = result.choices[0]
        
#         if choice.message.tool_calls is not None:
#             for tool_call in choice.message.tool_calls:
#                 # Parse arguments
#                 args_str = tool_call.function.arguments
#                 try:
#                     if isinstance(args_str, str):
#                         arguments = json.loads(args_str)
#                     elif isinstance(args_str, dict):
#                         arguments = args_str
#                     else:
#                         arguments = {"raw": str(args_str)}
#                 except Exception:
#                     arguments = {"raw": str(args_str)}
                
#                 tool_calls.append({
#                     "name": tool_call.function.name,
#                     "arguments": arguments
#                 })
#     except Exception as e:
#         print(f"Warning: Failed to extract tool calls: {e}")
    
#     return tool_calls


# # ============================================================
# # Global Instance
# # ============================================================

# # Create a single global instance
# # This will be shared across all tasks
# priority_tracker = DynamicPriorityTracker(
#     similarity_threshold=0.85,
#     log_file="priority_changes.jsonl"
# )


# # ============================================================
# # Convenience Functions
# # ============================================================

# def reset_task_priority(task_id: str):
#     """Reset priority tracking for a task."""
#     priority_tracker.reset_task(task_id)


# def get_task_priority_stats(task_id: str) -> Dict:
#     """Get current priority statistics for a task."""
#     return priority_tracker.get_stats(task_id)


# def configure_priority_system(
#     similarity_threshold: Optional[float] = None,
#     priority_thresholds: Optional[Dict[float, int]] = None
# ):
#     """
#     Configure the priority system.
    
#     Args:
#         similarity_threshold: Threshold for detecting similar tool calls
#         priority_thresholds: Mapping of avg_repetition -> priority level
#     """
#     if similarity_threshold is not None:
#         priority_tracker.configure_similarity(similarity_threshold)
    
#     if priority_thresholds is not None:
#         priority_tracker.configure_thresholds(priority_thresholds)