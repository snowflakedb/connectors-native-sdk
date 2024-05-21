SET APP_NAME = '&{APP_NAME}';

CREATE OR REPLACE DATABASE IDENTIFIER('&STAGE_DB');
CREATE STAGE IF NOT EXISTS IDENTIFIER('&STAGE_NAME');

PUT 'file://sf_build/*' @&{STAGE_NAME}/&APP_VERSION AUTO_COMPRESS = FALSE overwrite=true;

DROP APPLICATION PACKAGE IF EXISTS IDENTIFIER($APP_NAME);
CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER($APP_NAME);
ALTER APPLICATION PACKAGE IDENTIFIER($APP_NAME) ADD VERSION &APP_VERSION USING @&{STAGE_NAME}/&APP_VERSION;