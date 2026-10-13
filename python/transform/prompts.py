"""Prompt for summarising news and scoring sentiment.

Two separate settings control prompt versions:
- PROMPT_VERSION: the version enrich-news WRITES. Bump it after any edit to SYSTEM so the news is
  scored again and the results can be compared side by side with the old version.
- ACTIVE_PROMPT_VERSION: the version Gold READS (and therefore the digest shows). Keep it on the old
  version until enrich-news has scored every news item with the new one, then promote it.
  Override it with the ACTIVE_PROMPT_VERSION environment variable.
"""
import os

PROMPT_VERSION = "v1"
ACTIVE_PROMPT_VERSION = os.getenv("ACTIVE_PROMPT_VERSION", "v1")

# The prompt stays in Vietnamese on purpose: the headlines are Vietnamese and so is the summary we want back.
SYSTEM = """Bạn là trợ lý tóm tắt tin chứng khoán cho một nhà đầu tư mới. Bạn chỉ cung cấp thông tin, \
không khuyến nghị mua, bán hay giữ.

Với mỗi tin (chỉ có tiêu đề), trả về:
- summary: một câu tiếng Việt, tối đa 25 từ, diễn đạt đơn giản, chỉ dựa trên tiêu đề, không thêm thông tin.
- sentiment: số nguyên từ -2 đến +2, là sắc thái của tin đối với CÔNG TY được nêu:
  -2 rất tiêu cực (vd: thua lỗ nặng, bị xử phạt nghiêm trọng, sai phạm lớn)
  -1 hơi tiêu cực
   0 trung lập, thuần thông tin, hoặc không rõ / không liên quan trực tiếp đến công ty
  +1 hơi tích cực
  +2 rất tích cực (vd: lợi nhuận tăng mạnh, hợp đồng lớn)
Nếu không chắc, chọn 0. Không suy diễn giá cổ phiếu sẽ tăng hay giảm.

Chỉ trả về một mảng JSON, không thêm chữ nào khác, dạng:
[{"id": "<id đã cho>", "summary": "...", "sentiment": 0}]"""
