-- ============================================================
-- 크롤러가 anon(공개) 키로 저장할 수 있게 RLS 정책 추가
-- Supabase → SQL Editor 에서 프로젝트에 맞게 검토 후 실행
--
-- 주의: anon 키는 클라이언트에 노출될 수 있으므로,
--       이 정책은 "누구나 URL+anon 키로 해당 테이블 읽기/쓰기 가능"에 가깝습니다.
--       공개 앱·웹에 anon 키를 넣는 경우에는 더 좁은 정책이 필요합니다.
--       로컬 exe 전용이면 service_role 키(.env)만 쓰는 편이 더 단순할 수 있습니다.
-- ============================================================

-- ── parsing_wing_products ───────────────────────────────────
ALTER TABLE public.parsing_wing_products ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "crawler_anon_all_parsing_wing_products" ON public.parsing_wing_products;
CREATE POLICY "crawler_anon_all_parsing_wing_products"
  ON public.parsing_wing_products
  FOR ALL
  TO anon
  USING (true)
  WITH CHECK (true);

-- ── parsing_wing_options ─────────────────────────────────────
ALTER TABLE public.parsing_wing_options ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "crawler_anon_all_parsing_wing_options" ON public.parsing_wing_options;
CREATE POLICY "crawler_anon_all_parsing_wing_options"
  ON public.parsing_wing_options
  FOR ALL
  TO anon
  USING (true)
  WITH CHECK (true);

-- ── product_external_mappings (신규 상품 시 사용) ───────────
ALTER TABLE public.product_external_mappings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "crawler_anon_all_product_external_mappings" ON public.product_external_mappings;
CREATE POLICY "crawler_anon_all_product_external_mappings"
  ON public.product_external_mappings
  FOR ALL
  TO anon
  USING (true)
  WITH CHECK (true);
