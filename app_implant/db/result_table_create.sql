CREATE TABLE public.implant_prediction (
id serial NOT NULL PRIMARY KEY,
key_number varchar(200) NOT NULL,
prediction_time timestamp without time zone,
implant_prob numeric(5,2)
);
 
COMMENT ON TABLE public.implant_prediction IS '种植预测表';
COMMENT ON COLUMN public.implant_prediction.id IS '主键';
COMMENT ON COLUMN public.implant_prediction.key_number IS '关联号';
COMMENT ON COLUMN public.implant_prediction.prediction_time IS '预测时间';
COMMENT ON COLUMN public.implant_prediction.implant_prob IS '种植标签';
 
CREATE INDEX implant_prediction_implant_prob ON public.implant_prediction USING btree (implant_prob);