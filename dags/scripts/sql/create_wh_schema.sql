CREATE SCHEMA IF NOT EXISTS wh;

CREATE TABLE IF NOT EXISTS wh.dim_image (
    image VARCHAR(255),
    symbol VARCHAR(10)
);

INSERT INTO wh.dim_image (image, symbol)
VALUES
('https://sudospaces.com/hoaphat-com-vn/2019/10/hpg-logo-cymk-0211-artboard-7-copy-2.jpg', 'HPG'),
('https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTa8jssMy7VTJxGB8hFWHzeiZ48k4DMvDIyuQ&s', 'VNM'),
('https://upload.wikimedia.org/wikipedia/commons/thumb/1/11/FPT_logo_2010.svg/1200px-FPT_logo_2010.svg.png', 'FPT'),
('https://cdn.haitrieu.com/wp-content/uploads/2022/02/Logo-Vietcombank.png', 'VCB'),
('https://static.wixstatic.com/media/9d8ed5_0a9b18a750bc43c3bfc35824856331f7~mv2.jpg/v1/fill/w_568,h_284,al_c,q_80,usm_0.66_1.00_0.01,enc_avif,quality_auto/9d8ed5_0a9b18a750bc43c3bfc35824856331f7~mv2.jpg', 'BID'),
('https://cdn.haitrieu.com/wp-content/uploads/2021/11/Logo-TCB-V.png', 'TCB'),
('https://cdn.haitrieu.com/wp-content/uploads/2022/01/Logo-VietinBank-CTG-Ori.png', 'CTG'),
('https://rubee.com.vn/wp-content/uploads/2021/05/logo-vinhomes.jpg', 'VHM'),
('https://media.loveitopcdn.com/3807/logo-masan-group-compressed.jpg', 'MSN'),
('https://cdn.haitrieu.com/wp-content/uploads/2021/11/Logo-The-Gioi-Di-Dong-MWG.png', 'MWG');
