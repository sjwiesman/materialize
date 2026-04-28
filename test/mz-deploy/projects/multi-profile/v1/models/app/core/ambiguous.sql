-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- PRAGMA WARN_ON_MISSING_VARIABLES;
CREATE VIEW ambiguous AS
SELECT
    arr[:b] AS arr_slice,
    b
FROM (
    VALUES
        (LIST[10,20,30,40,50], 2),
        (LIST[5,6,7,8], 3),
        (LIST[100,200,300], 1)
) AS t(arr, b);
