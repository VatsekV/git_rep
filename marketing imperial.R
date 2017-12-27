  
  # Intro----------------------------
  
  Sys.setenv(tz = "gmt")
  options(stringsAsFactors = F)
  library(dplyr)
  library(stringr)
  library(RPostgreSQL)
  
  
  # Functions and files---------------
  QualitySetFunction <- function(vctr) {
    ifelse(grepl("_AE_",vctr, ignore.case = T),"AE",
           ifelse(grepl("_KW_",vctr, ignore.case = T) & grepl("LAL",vctr, ignore.case = T),"LAL",
                  ifelse(grepl("_KW_",vctr, ignore.case = T),"KW",
                         ifelse(grepl("_VGE_",vctr, ignore.case = T),"VGE",
                                ifelse(grepl("_VGA_",vctr, ignore.case = T),"VGA",
                                       ifelse(grepl("_VG_",vctr, ignore.case = T),"VG",
                                              ifelse(grepl("_EG_",vctr, ignore.case = T),"EG",
                                                     ifelse(grepl("_LAL_", vctr, ignore.case = T),"LAL",
                                                            ifelse(grepl("_aeo_", vctr, ignore.case = T),"AEO",
                                                                   ifelse(grepl("_NT_",vctr, ignore.case = T),"NT","Unknown")
                                                            )
                                                     )
                                              )
                                       )
                                )
                         )
                  )
           )
    )
  }
  ManagerSetFunction <- function(vctr) {
    ifelse(grepl("ADIM",vctr, ignore.case = T),"Alina",
           ifelse(grepl("VKIM",vctr, ignore.case = T),"Vadim",
                  ifelse(grepl("EKIM",vctr, ignore.case = T),"Ekaterina",
                         ifelse(grepl("MCIM",vctr, ignore.case = T),"Maksim",
                                ifelse(grepl("AKIM",vctr, ignore.case = T),"Arseniy",
                                       ifelse(grepl("SNIM",vctr, ignore.case = T),"AleksandrN","Unknown")
                                       )
                                )
                         )
                  )
           )
  
  }
  AdPlatformSetFunction <- function(vctr) {
    ifelse(grepl("pall",vctr, ignore.case = T),"All",
           ifelse(grepl("fb",vctr, ignore.case = T),"Facebook",
                  ifelse(grepl("ins",vctr, ignore.case = T),"Instagramm",
                         ifelse(grepl("net",vctr, ignore.case = T),"Audience_Network",
                                ifelse(grepl("mes",vctr, ignore.case = T),"Messegger",
                                       ifelse(grepl("vk",vctr, ignore.case = T),"Vkontakte",
                                              ifelse(grepl("od",vctr, ignore.case = T),"Odnoklassniki",
                                                     ifelse(grepl("mr",vctr, ignore.case = T),"MailRu",
                                                          ifelse(grepl("all",vctr, ignore.case = T),"All","Unknown")
                                                     )
                                              )
                                       )
                                )
                         )
                  )
           )
    )
    
  }
  ShapeSetFunction <- function(vctr){
    ifelse(grepl("^v",vctr, ignore.case = T),"Video",
                ifelse(grepl("^s",vctr, ignore.case = T),"Static",
                       ifelse(grepl("^d",vctr, ignore.case = T),"Dynamic","Unknown")
                )
           )
  }
  ReplaceAdnamefunction <- function(vctr) {
    ifelse(grepl("h001t0023dGF0001",vctr, ignore.case = T),"dGF0001",
           ifelse(grepl("h002t0009sAR0034",vctr, ignore.case = T),"sAR0034",
                  ifelse(grepl("h004t0020dCG0004",vctr, ignore.case = T),"dCG0004",
                         ifelse(grepl("h004t0020sAR0073",vctr, ignore.case = T),"sAR0073",vctr)
                  )
           )
    )
  }
    
    
  con_v4  <-  dbConnect("PostgreSQL",dbname="analytics_v4",
                     user="user_ro",
                     password="",
                     host="127.0.0.1",
                     port="5432")
  
  con_spend  <-  dbConnect("PostgreSQL",dbname="tablo_spend",
                     user="user_ro",
                     password="",
                     host="127.0.0.1",
                     port="5432")
  
  geo_tbl <- read.csv(file="C:/Users/user/Google ????/09.2017/??????? ????????/GEO 2.csv",sep=';')
  # 
  # geo_tbl <- xlsx::read.xlsx(file="C:/Users/user-c015/Google Drive/09.2017/?????????????? ????????????????/GEO.xlsx", 
  #                            sheetIndex=1,
  #                            stringsAsFactors=F)
  geo_tbl$V1 <- gsub(pattern = " ", replacement = "" ,x = geo_tbl$V1)
  geo_tbl$V2 <- gsub(pattern = " ", replacement = "" ,x = geo_tbl$V2)
  geo_tbl$V1 <- paste0("_",geo_tbl$V1, "_")
  
  target_tbl <- read.csv("C:/Users/user/Google ????/Imperial tablo csv data/campaign_names_target.csv", sep=";")
  target_tbl <- target_tbl[,c(1,2)]
  target_tbl <- unique(target_tbl)
  colnames(target_tbl) <- c("Campaign_ID","Campaign_Name")
  
  testers <- data.frame(id=c(62281079,61097680,61189020,61190947,61198556,61214890,61234588,61234653,61234654,61275984,61289940,61339553,62354094,62354358,62355307,62356666,62356667,62358134,62376397,62409500,65507157,65605381,67289651,66625493))
  
  
  # Load from DB -----------------------
  
  imperial_lvl_ups <- dbGetQuery( con_v4,"
                                  select
                                  u.application_id as Application_id,
                                  date_trunc('day', case when ug.p[3]='''mail.ru_int''' then u.reg_date + interval '3 hour' else u.reg_date end ) as Reg_date,
                                  case when a.media_source is not null then a.media_source else 'Organic' end as Media_source,
                                  case when a.af_channel is not null then a.af_channel else 'Non' end as Ad_platform,
                                  case 
                                        when a.media_source='adcolony_int'  then a.af_c_id 
    		                                when a.media_source='Facebook Ads' then a.af_adset else
                                          case when a.campaign is not null then a.campaign else 'Non' end end as Compaign,
                                  case when a.af_ad is not null then a.af_ad else 'Non' end as Ad,
                                  case when a.af_ad_type is not null then a.af_ad_type else 'Non' end as Placement,
                                  case 
                                      when a.platform is not null then( 
                                      case 
                                          when a.platform='ios' then 'iOS'
                                          when a.platform='android' then 'Android'
                                          else 'Unknown' end)
                                      else replace(ug.p[6],'''','') end as Device_platform,
                                  case 
                                    when a.device_type like ('%iPhone%') then 'iPhone'
                                    when a.device_type like ('%iPad%') then 'iPad'
                                    when a.device_type is null then 'Unknown'
                                    else 'Android phone or tablet' end as Device_type,
                                  -- replace(p.p[1],'''','') as lvl,
                                  sum(case 
                                  when replace(p.p[1],'''','')::int >2 then 1 
                                  else null end) as lvlup1,
                                  sum(case 
                                  when replace(p.p[1],'''','')::int >3 then 1 
                                  else null end) as lvlup3,
                                  sum(case 
                                  when replace(p.p[1],'''','')::int >7 then 1 
                                  else null end) as lvlup7,
                                  sum(case 
                                  when replace(p.p[1],'''','')::int >8 then 1
                                  else null end) as lvlup8,
                                  sum(case 
                                  when replace(p.p[1],'''','')::int >10 then 1
                                  else null end) as lvlup10,
                                  count (*) as installs
                                  from users u
                                  left join progress p on u.progress_id=p.id
                                  left join attribution a on a.user_id=u.id
                                  left join user_groups ug on ug.id=u.user_group_id
                                  where u.application_id=52
                                    and u.reg_date>'2017-09-01'	
                                    and u.user_group_id!=380270
                                  group by 1,2,3,4,5,6,7,8,9")
  
  
  imperial_ret <- dbGetQuery( con_v4,"
                                select
                                  u.application_id as Application_id,
                              date_trunc('day', case when ug.p[3]='''mail.ru_int''' then u.reg_date + interval '3 hour' else u.reg_date end ) as Reg_date,
                              case when a.media_source is not null then a.media_source else 'Organic' end as Media_source,
                              case when a.af_channel is not null then a.af_channel else 'Non' end as Ad_platform,
                              case when a.media_source='adcolony_int' 
    		                        then a.af_c_id when a.media_source='Facebook Ads' then a.af_adset else
                                case when a.campaign is not null then a.campaign else 'Non' end end as Compaign,
                              case when a.af_ad is not null then a.af_ad else 'Non' end as Ad,
                              case when a.af_ad_type is not null then a.af_ad_type else 'Non' end as Placement,
                              case when a.platform is not null then( 
                              case when a.platform='ios' then 'iOS'
                              when a.platform='android' then 'Android'
                              else 'Unknown' end)
                              else replace(ug.p[6],'''','') end as Device_platform,
                              case 
                                  when a.device_type like ('%iPhone%') then 'iPhone'
                                  when a.device_type like ('%iPad%') then 'iPad'
                                  when a.device_type is null then 'Unknown'
                                  else 'Android phone or tablet' end as Device_type,
                              count(distinct (case when S.date::date = U.reg_date::date + 1 then U.id else null end)) as ret_1,
                              count(distinct (case when S.date::date = U.reg_date::date + 3 then U.id else null end)) as ret_3,
                              count(distinct (case when S.date::date = U.reg_date::date + 7 then U.id else null end)) as ret_7,
                              count(distinct (case when S.date::date = U.reg_date::date + 14 then U.id else null end)) as ret_14,
                              count(distinct (case when S.date::date = U.reg_date::date + 28 then U.id else null end)) as ret_28,
                              count (distinct u.id) as installs
                              from users u
                              left join attribution a on a.user_id=u.id
                              left join sessions s on s.user_id=u.id
                              left join user_groups ug on ug.id=u.user_group_id
                              where u.application_id=52	
                              and u.reg_date>'2017-09-01'
                              and u.user_group_id!=380270
                              group by 1,2,3,4,5,6,7,8,9")
  
  imperial_pays <- dbGetQuery( con_v4,paste( "
                                select
                                  u.application_id as Application_id,
                               date_trunc('day', case when ug.p[3]='''mail.ru_int''' then u.reg_date + interval '3 hour' else u.reg_date end ) as Reg_date,
                               case when a.media_source is not null then a.media_source else 'Organic' end as Media_source,
                               case when a.af_channel is not null then a.af_channel else 'Non' end as Ad_platform,
                               case when a.media_source='adcolony_int' 
    		                          then a.af_c_id when a.media_source='Facebook Ads' then a.af_adset else
                                  case when a.campaign is not null then a.campaign else 'Non' end end as Compaign,
                               case when a.af_ad is not null then a.af_ad else 'Non' end as Ad,
                               case when a.af_ad_type is not null then a.af_ad_type else 'Non' end as Placement,
                               case when a.platform is not null then( 
                               case when a.platform='ios' then 'iOS'
                               when a.platform='android' then 'Android'
                               else 'Unknown' end)
                               else replace(ug.p[6],'''','') end as Device_platform,
                               case 
                               when a.device_type like ('%iPhone%') then 'iPhone'
                               when a.device_type like ('%iPad%') then 'iPad'
                               when a.device_type is null then 'Unknown'
                               else 'Android phone or tablet' end as Device_type,
                               count(distinct (case when p.date < u.reg_date + interval '1 day' then u.id else null end)) as payers_1,
                               count(distinct (case when p.date < u.reg_date + interval '3 day' then u.id else null end)) as payers_3,
                               count(distinct (case when p.date < u.reg_date + interval '7 day' then u.id else null end)) as payers_7,
                               count(distinct (case when p.date < u.reg_date + interval '14 day' then u.id else null end)) as payers_14,
                               count(distinct (case when p.date < u.reg_date + interval '30 day' then u.id else null end)) as payers_30,
                               count(distinct (case when p.date < u.reg_date + interval '60 day' then u.id else null end)) as payers_60,
                               count(distinct p.user_id) as payer_999,
                               sum(case when p.date < u.reg_date + interval '1 day' then p.amount*0.0000007 else null end) as money_1,
                               sum(case when p.date < u.reg_date + interval '3 day' then p.amount*0.0000007 else null end) as money_3,
                               sum(case when p.date < u.reg_date + interval '7 day' then p.amount*0.0000007 else null end) as money_7,
                               sum(case when p.date < u.reg_date + interval '14 day' then p.amount*0.0000007 else null end) as money_14,
                               sum(case when p.date < u.reg_date + interval '30 day' then p.amount*0.0000007 else null end) as money_30,
                               sum(case when p.date < u.reg_date + interval '60 day' then p.amount*0.0000007 else null end) as money_60,
                               sum(p.amount*0.0000007) as money_999,
                               count (distinct u.id) as installs
                               from users u
                               left join attribution a on a.user_id=u.id
                               left join payments p on p.user_id=u.id
                               left join user_groups ug on ug.id=u.user_group_id
                               where u.application_id=52	
                                and u.reg_date>'2017-09-01'
                                and u.id not in (",paste(testers$id,collapse=","),")
                               group by 1,2,3,4,5,6,7,8,9"))
  
  imperial_tutor <- dbGetQuery( con_v4,paste( "
                                select
                                  u.application_id as Application_id,
                                date_trunc('day', case when ug.p[3]='''mail.ru_int''' then u.reg_date + interval '3 hour' else u.reg_date end ) as Reg_date,
                                case when a.media_source is not null then a.media_source else 'Organic' end as Media_source,
                                case when a.af_channel is not null then a.af_channel else 'Non' end as Ad_platform,
                                case when a.media_source='adcolony_int' 
    		                          then a.af_c_id when a.media_source='Facebook Ads' then a.af_adset else
                                  case when a.campaign is not null then a.campaign else 'Non' end end as Compaign,
                                case when a.af_ad is not null then a.af_ad else 'Non' end as Ad,
                                case when a.af_ad_type is not null then a.af_ad_type else 'Non' end as Placement,
                                case when a.platform is not null then( 
                                case when a.platform='ios' then 'iOS'
                                when a.platform='android' then 'Android'
                                else 'Unknown' end)
                                else replace(ug.p[6],'''','') end as Device_platform,
                                case 
                                when a.device_type like ('%iPhone%') then 'iPhone'
                                when a.device_type like ('%iPad%') then 'iPad'
                                when a.device_type is null then 'Unknown'
                                else 'Android phone or tablet' end as Device_type,
                              sum(t.is_tutor_complete) as complete_tutor,
                              count (distinct u.id) as installs
                              from users u
                              left join attribution a on a.user_id=u.id
                              left join payments p on p.user_id=u.id
                              left join user_groups ug on ug.id=u.user_group_id
                              left join (select
                              				u.id,
                              				1 as is_tutor_complete
                              				from users u, events e
                              				where u.application_id=52
                              					and e.user_id=u.id
                              					and e.date>'2017-09-01'
                              					and u.reg_date between '2017-09-01' and '2018-01-01'
                              					and e.p[8]=87850063
                              					and e.type=409302) t on t.id=u.id
                              where u.application_id=52
                                and u.reg_date>'2017-09-01'
                                and u.id not in (",paste(testers$id,collapse=","),")
                              group by 1,2,3,4,5,6,7,8,9"))
  
  
  # Join all together ------------
  
  imperial_all <- left_join(imperial_lvl_ups,imperial_ret, by = c("reg_date","media_source","ad_platform","compaign","ad","placement","device_platform","device_type","application_id"))
  imperial_all <- left_join(imperial_all,imperial_pays, by = c("reg_date","media_source","ad_platform","compaign","ad","placement","device_platform","device_type","application_id"))
  imperial_all <- left_join(imperial_all,imperial_tutor, by = c("reg_date","media_source","ad_platform","compaign","ad","placement","device_platform","device_type","application_id"))

  # Repair fail ---------
  failrepairfunction <- function(vctr) {
    ifelse(grepl("FB_inst_SNIM_an_mix__RUS_LAL_ret>3d_1865__M_ru__24/11",vctr, ignore.case = T),"FB_inst_AKIM_an_mix__RUS_LAL_ret>3d_1865__M_ru__24/11",
           ifelse(grepl("FB_inst_SNIM_an_mix__RUS_KW_pcstrategies_1865__M_ru__24/11",vctr, ignore.case = T),"FB_inst_AKIM_an_mix__RUS_KW_pcstrategies_1865__M_ru__24/11",
                  ifelse(grepl("FB_pall_SNIM_an_mix__RUS_AEO_checkout_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_AEO_checkout_1865__M_ru__22/11",
                         ifelse(grepl("FB_pall_SNIM_an_mix__RUS_LAL_ret>3d_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_LAL_ret>3d_1865__M_ru__22/11",
                                ifelse(grepl("FB_pall_SNIM_an_mix__RUS_KW_pcstrategies_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_KW_pcstrategies_1865__M_ru__22/11",
                                       ifelse(grepl("FB_pall_SNIM_an_mix__RUS_LAL_purchases>1last50d_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_LAL_purchases>1last50d_1865__M_ru__22/11",
                                              ifelse(grepl("FB_pall_SNIM_an_mix__RUS_AEO_purchase_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_AEO_purchase_1865__M_ru__22/11",
                                                     ifelse(grepl("FB_pall_SNIM_an_mix__RUS_AEO_tutor_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_AKIM_an_mix__RUS_AEO_tutor_1865__M_ru__22/11",
                                                            ifelse(grepl("FB_Pall_AKIM_io_ph__RUS_LAL_RetMore7d_1865__M_ru__23/11_",vctr, ignore.case = T),"FB_Pall_SNIM_io_ph__RUS_LAL_RetMore7d_1865__M_ru__23/11_",
                                                                   ifelse(grepl("FB_Pall_AKIM_io_ph__RUS_LAL_IoRetMore7d_1865__M_ru__24/11",vctr, ignore.case = T),"FB_Pall_SNIM_io_ph__RUS_LAL_IoRetMore7d_1865__M_ru__24/11",
                                                                          ifelse(grepl("FB_Pall_AKIM_io_ph__RUS_LAL_RetMore7d_1865__M_ru__23/11_2%",vctr, ignore.case = T),"FB_Pall_SNIM_io_ph__RUS_LAL_RetMore7d_1865__M_ru__23/11_2%",
                                                                                 ifelse(grepl("FB_ph_AKIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__23/11_",vctr, ignore.case = T),"FB_ph_SNIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__23/11_",
                                                                                        ifelse(grepl("FB_Pall_AKIM_io_mix__RUS_AEO_registration_1865__M_ru__22/11_",vctr, ignore.case = T),"FB_Pall_SNIM_io_mix__RUS_AEO_registration_1865__M_ru__22/11_",
                                                                                               ifelse(grepl("FB_inst_AKIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__24/11_feed",vctr, ignore.case = T),"FB_inst_SNIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__24/11_feed",
                                                                                                      ifelse(grepl("FB_Pall_AKIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__23/11_2%",vctr, ignore.case = T),"FB_Pall_SNIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__23/11_2%",
                                                                                                             ifelse(grepl("FB_pall_EKIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__22/11",vctr, ignore.case = T),"FB_pall_SNIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__22/11",
                                                                                                                    ifelse(grepl("FB_pall_EKIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__17/11",vctr, ignore.case = T),"FB_pall_EKIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__22/11",
                                                                                                                           ifelse(grepl("FB_pall_EKIM_io_mix__RUS_LAL_CrisisPayersRU_1865__M_ru__17/11",vctr, ignore.case = T),"FB_pall_EKIM_io_mix__RUS_LAL_CrisisPayersRU_1865__M_ru__22/11",
                                                                                                                                  ifelse(grepl("FB_pall_EKIM_io_mix__RUS_AEO_Addtowishlist_1865__M_ru__17/11",vctr, ignore.case = T),"FB_pall_EKIM_io_mix__RUS_AEO_Addtowishlist_1865__M_ru__22/11",
                                                                                                                                         ifelse(grepl("FB_pall_EKIM_io_mix__RUS_LAL_sessions100after7d_1865__M_ru__17/11",vctr, ignore.case = T),"FB_pall_EKIM_io_mix__RUS_LAL_sessions100after7d_1865__M_ru__22/11",
                                                                                                                                                ifelse(grepl("FB_pall_EKIM_an_mix__RUS_KW_socstrategies_1865__M_ru__17/11",vctr, ignore.case = T),"FB_pall_EKIM_an_mix__RUS_KW_socstrategies_1865__M_ru__22/11",
                                                                                                                                                       ifelse(grepl("FB_Pall_EKIM_an_mix__RUS_LAL_sessions100after7d_1865__M_ru__17/11",vctr, ignore.case = T),"FB_Pall_EKIM_an_mix__RUS_LAL_sessions100after7d_1865__M_ru__22/11",
                                                                                                                                                              ifelse(grepl("FB_ph_AKIM_io_mix__RUS_LAL_sessions100last30d_1865__M_ru__23/11_",vctr, ignore.case = T),"FB_ph_SNIM_io_ph__RUS_LAL_sessions100last30d_1865__M_ru__23/11_",
                                                                                                                                                                     ifelse(grepl("FB_Pall_EKIM_an_mix__RUS_LAL_sessions100after4d_1865__M_ru__17/11",vctr, ignore.case = T),"FB_Pall_EKIM_an_mix__RUS_LAL_sessions100after4d_1865__M_ru__22/11",
                                                                                                                                                                            ifelse(grepl("FB_inst_EKIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__17/11",vctr, ignore.case = T),"FB_inst_SNIM_io_mix__RUS_LAL_sessions100after4d_1865__M_ru__24/11",vctr
                                                                                                                                                                            )))))))))))))))))))))))))
  }
  fail<- read.csv("C:/Users/user/Google ????/Imperial tablo csv data/fail.csv", sep = ";")
  imperial_all$compaign <- failrepairfunction(imperial_all$compaign)
  # Load spent from FB and join all together ----
   #FB----
  
  spent <- read.csv("C:/Users/user/Google ????/Imperial tablo csv data/lifetime_fb.csv")
  #spent <- read.csv("/Users/cu037/Google Drive/09.2017/Скрипты ?мпериал/Imperial-USD-Ads-Lifetime.csv")
  spent1 <- spent[,c(1,3,4,5,6,8,9,10,11,12)]
  colnames(spent1) <- c("reg_date","compaign","ad","ad_platform","placement","device_type","impressions","uniquelinkclicks","spent","installs")
  spent1$media_source <- "Facebook Ads"
  spent1$reg_date <- as.POSIXct(spent1$reg_date)
  spent1$device_platform <- ifelse(grepl("iP",spent1$device_type, ignore.case = T),"iOS","Android")
  spent1$device_type <- ifelse(grepl("Android",spent1$device_type, ignore.case = T),"Android phone or tablet",spent1$device_type)
  #spent1$placement <- "Feed"
  spent1$placement[spent1$ad_platform=='Facebook'] <- "Feed"
  spent1[is.na(spent1)] <- 0
  
  spent1 <- spent1 %>% 
    group_by(reg_date,ad,ad_platform,placement,device_type,compaign,media_source,device_platform) %>%
    summarise(impressions=sum(impressions),uniquelinkclicks=sum(uniquelinkclicks),spent=sum(spent),installs=sum(installs)) %>%
    ungroup()
  
  
  imperial_all$placement[imperial_all$ad_platform=='AudienceNetwork'] <- ifelse(grepl("rewarded_video",imperial_all$placement[imperial_all$ad_platform=='AudienceNetwork'], ignore.case = T),"Rewarded Video","Native, Banner & Interstitial")
  imperial_all$placement[imperial_all$ad_platform=='Instagram'] <- ifelse(grepl("stories",imperial_all$placement[imperial_all$ad_platform=='Instagram'], ignore.case = T),"Stories","Feed: News Feed")
  imperial_all$placement[imperial_all$ad_platform=='Messenger'] <- "Messenger Home"
  imperial_all$placement[imperial_all$ad_platform=='Facebook'] <- "Feed"
  #imperial_all$placement[imperial_all$media_source=='Facebook Ads'] <- "Feed"
  imperial_all$ad_platform[imperial_all$ad_platform=='AudienceNetwork'] <- "Audience Network"
  
  #imperial_all <- imperial_all %>% 
  #  group_by(application_id,reg_date,media_source,ad_platform,compaign,ad,placement,device_platform,device_type) %>%
  #  summarise_each(funs(sum)) %>%
  #  ungroup()
  
  
  imperial_organic <- subset.data.frame(imperial_all, media_source=="Organic")
  spent1 <- left_join(spent1,imperial_all)
  
  imperial_marketing <- merge(spent1,imperial_organic,all = TRUE)
  
   #Unity ----
  
  imperial_unity <- subset.data.frame(imperial_all, media_source=="unityads_int")
  spent_unity <- read.csv("C:/Users/user/Google ????/09.2017/??????? ????????/imperial_unity.csv")
  for (i in 1:nrow(spent_unity)) {
    spent_unity$compaign[i] <- strsplit(spent_unity$Campaign.name[i],"-")[[1]][2]
  #  spent_unity$ad[i] <- strsplit(spent_unity$Campaign.name[i],"-")[[1]][3]
  }
  spent_unity$reg_date <- as.POSIXct(spent_unity$Date)
  spent_unity <- spent_unity[,c(14,13,8,10,12)]
  colnames(spent_unity) <- c('reg_date','compaign','impressions','clicks','spend')
  
  spent_unity <- spent_unity %>% 
    group_by(reg_date,compaign) %>%
    summarise(impressions=sum(impressions),uniquelinkclicks=sum(clicks),spent=sum(spend)) %>%
    ungroup()
  
  imperial_unity <- subset(imperial_unity, select = -ad )
  imperial_unity <- imperial_unity %>% 
    group_by(application_id,reg_date,media_source,ad_platform,compaign,placement,device_platform,device_type) %>%
    summarise_all(funs(sum)) %>%
    ungroup()
  
  imperial_unity <- left_join(spent_unity,imperial_unity)
  
  imperial_marketing <- merge(imperial_marketing,imperial_unity,all = TRUE)
  
   #myTarget----
  
  spent_target <- read.csv("C:/Users/user/Google ????/Imperial tablo csv data/lifetime_target.csv", encoding = "UTF-8")
  colnames(spent_target) <- c("reg_date","compaign","age_mytarget","sex_mytarget","impressions","uniquelinkclicks","installs","spent","ctr","cr","cta")
  spent_target <- spent_target[,c(1,2,5,6,8)]
  spent_target$reg_date <- as.Date(spent_target$reg_date)
  spent_target$reg_date <- as.POSIXct(spent_target$reg_date)
  
  imperial_target <- subset.data.frame(imperial_all, media_source=="mail.ru_int")
  for (i in 1:nrow(imperial_target)) {
    imperial_target$compaign[i] <- strsplit(imperial_target$compaign[i],"_")[[1]][1]
  }
  imperial_target$device_type[imperial_target$device_type=='iPad'] <- "iPhone or iPad"
  imperial_target$device_type[imperial_target$device_type=='iPhone'] <- "iPhone or iPad"
  imperial_target <- subset(imperial_target, select = -ad )
  
  imperial_target[is.na(imperial_target)] <- 0
  imperial_target <- imperial_target %>% 
    group_by(application_id,reg_date,media_source,ad_platform,compaign,placement,device_platform,device_type) %>%
    summarise_all(funs(sum)) %>%
    ungroup()
  
  spent_target[is.na(spent_target)] <- 0
  spent_target <- spent_target %>% 
    group_by(reg_date,compaign) %>%
    summarise_all(funs(sum)) %>%
    ungroup()
  
  spent_target$compaign <- as.character(spent_target$compaign)
  
  # new 
  
  spent_target$row_num <- row.names(spent_target)
  t_all <- left_join(imperial_target,spent_target)
  t_not_all <- subset(spent_target, !(spent_target$row_num %in% t_all$row_num))
  t_all_all <- merge(t_all,t_not_all,all = TRUE)
  t_all_all <- subset(t_all_all, select = -row_num)
  t_all_all$media_source[is.na(t_all_all$media_source)] <- "mail.ru_int"
  imperial_target <- t_all_all
  rm(t_all,t_not_all,t_all_all)
  
  # old 
  #imperial_target <- left_join(spent_target,imperial_target)
  
  imperial_target <- merge(imperial_target,target_tbl, by.x="compaign", by.y="Campaign_ID")
  imperial_target$compaign <- imperial_target$Campaign_Name
  imperial_target <- subset(imperial_target, select = -Campaign_Name )
  
  imperial_marketing <- merge(imperial_marketing,imperial_target,all = TRUE)
  
   #AdColony----
  spent_adcolony <- read.csv("C:/Users/user/Google ????/09.2017/??????? ????????/adcolony_lifetime.csv")
  spent_adcolony <- spent_adcolony[,c(1,5,6,12,13)]
  colnames(spent_adcolony) <- c("reg_date","compaign","impressions","uniquelinkclicks","spent")
  spent_adcolony$reg_date <- as.Date(spent_adcolony$reg_date)
  spent_adcolony$reg_date <- as.POSIXct(spent_adcolony$reg_date)
  
  imperial_adcolony <- subset.data.frame(imperial_all, media_source=="adcolony_int")
  imperial_adcolony$device_type[imperial_adcolony$device_type=='iPad'] <- "iPhone or iPad"
  imperial_adcolony$device_type[imperial_adcolony$device_type=='iPhone'] <- "iPhone or iPad"
  imperial_adcolony[is.na(imperial_adcolony)] <- 0
  imperial_adcolony <- imperial_adcolony %>% 
    group_by(application_id,reg_date,media_source,ad_platform,compaign,ad,placement,device_platform,device_type) %>%
    summarise_all(funs(sum)) %>%
    ungroup()
  
  
  spent_adcolony$row_num <- row.names(spent_adcolony)
  t_all <- left_join(imperial_adcolony,spent_adcolony)
  t_not_all <- subset(spent_adcolony, !(spent_adcolony$row_num %in% t_all$row_num))
  t_all_all <- merge(t_all,t_not_all,all = TRUE)
  t_all_all <- subset(t_all_all, select = -row_num)
  t_all_all$media_source[is.na(t_all_all$media_source)] <- "adcolony_int"
  imperial_adcolony <- t_all_all
  rm(t_all,t_not_all,t_all_all)
  imperial_adcolony[is.na(imperial_adcolony)] <- 0
  
  
  imperial_marketing <- merge(imperial_marketing,imperial_adcolony,all = TRUE)
  
   #Vungle----
  spent_vungle <- read.csv("C:/Users/user/Google ????/09.2017/??????? ????????/vungle_lifetime.csv")
  spent_vungle <- spent_vungle[,c(1,6,11,14,19)]
  colnames(spent_vungle) <- c("reg_date","compaign","impressions","uniquelinkclicks","spent")
  spent_vungle$reg_date <- as.Date(spent_vungle$reg_date)
  spent_vungle$reg_date <- as.POSIXct(spent_vungle$reg_date)
  spent_vungle$spent <- gsub("\\$", "", spent_vungle$spent)
  spent_vungle$spent <- as.numeric(spent_vungle$spent)
  
  imperial_vungle <- subset.data.frame(imperial_all, media_source=="vungle_int")
  imperial_vungle$device_type[imperial_vungle$device_type=='iPad'] <- "iPhone or iPad"
  imperial_vungle$device_type[imperial_vungle$device_type=='iPhone'] <- "iPhone or iPad"
  imperial_vungle[is.na(imperial_vungle)] <- 0
  imperial_vungle <- imperial_vungle %>% 
    group_by(application_id,reg_date,media_source,ad_platform,compaign,ad,placement,device_platform,device_type) %>%
    summarise_all(funs(sum)) %>%
    ungroup()
  
  
  spent_vungle$row_num <- row.names(spent_vungle)
  t_all <- left_join(imperial_vungle,spent_vungle)
  t_not_all <- subset(spent_vungle, !(spent_vungle$row_num %in% t_all$row_num))
  t_all_all <- merge(t_all,t_not_all,all = TRUE)
  t_all_all <- subset(t_all_all, select = -row_num)
  t_all_all$media_source[is.na(t_all_all$media_source)] <- "vungle_int"
  imperial_vungle <- t_all_all
  rm(t_all,t_not_all,t_all_all)
  imperial_vungle[is.na(imperial_vungle)] <- 0
  
  
  imperial_marketing <- merge(imperial_marketing,imperial_vungle,all = TRUE)
   #and other ----
  
  imperial_other <- subset.data.frame(imperial_all, media_source!="Organic"&media_source!="Facebook Ads"&media_source!="unityads_int"&media_source!="mail.ru_int"&media_source!="adcolony_int"&media_source!="vungle_int")
  imperial_marketing <- merge(imperial_marketing,imperial_other,all = TRUE)
  
  
  
  # Add dimentions ------------
  
  imperial_marketing$manager <- ManagerSetFunction(imperial_marketing$compaign)
  imperial_marketing$quality <- QualitySetFunction(imperial_marketing$compaign)
  imperial_marketing$shape <- ShapeSetFunction(imperial_marketing$ad)
  imperial_marketing$geo <- 'Unknown'
  for (i in 1:nrow(geo_tbl)) {
    imperial_marketing$geo <- 
      ifelse(grepl(pattern = geo_tbl$V1[i], x = imperial_marketing$compaign),
             geo_tbl$V2[i],
             imperial_marketing$geo)
  }
  
  # link dimentions old, before 2017-09-27
  # for (i in 1:nrow(imperial_marketing)) {
  # imperial_marketing$link_platform[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][1]
  # imperial_marketing$link_manager[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][2]
  # imperial_marketing$link_os[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][3]
  # imperial_marketing$link_device[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][4]
  # imperial_marketing$link_geo[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][5]
  # imperial_marketing$link_quality[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][6]
  # imperial_marketing$link_placement[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][7]
  # imperial_marketing$link_audience[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][8]
  # imperial_marketing$link_age[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][9]
  # imperial_marketing$link_additional_targeting[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][10]
  # imperial_marketing$link_date[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][11]
  # }
  
  # link dimentions new, after 2017-09-27
imperial_marketing$compaign <- tolower(imperial_marketing$compaign)
  for (i in 1:nrow(imperial_marketing)) {
    imperial_marketing$link_media_source[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][1]
    imperial_marketing$link_ad_platform[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][2]
    imperial_marketing$link_manager[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][3]
    imperial_marketing$link_device_os[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][4]
    imperial_marketing$link_device_type[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][5]
    imperial_marketing$link_placement[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][6]
    imperial_marketing$link_geo[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][7]
    imperial_marketing$link_quality[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][8]
    imperial_marketing$link_audience[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][9]
    imperial_marketing$link_age[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][10]
    imperial_marketing$link_ads_type[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][11]
    imperial_marketing$link_sex[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][12]
    imperial_marketing$link_language[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][13]
    imperial_marketing$link_add_targeting[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][14]
    imperial_marketing$link_date[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][15]
    imperial_marketing$link_add_param[i] <- strsplit(imperial_marketing$compaign[i],"_")[[1]][16]
  }
  
  imperial_marketing$link_ad_platform <- AdPlatformSetFunction(imperial_marketing$link_ad_platform)
  
 # ad param 
  for (i in 1:nrow(imperial_marketing)) {
    imperial_marketing$ad_name[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][1]
    imperial_marketing$ad_store_icon[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][2]
    imperial_marketing$ad_logo[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][3]
    imperial_marketing$ad_lang[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][4]
    imperial_marketing$ad_timing[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][5]
    imperial_marketing$ad_size[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][6]
    imperial_marketing$ad_head_text[i] <- strsplit(imperial_marketing$ad[i],"_")[[1]][7]
  }
  imperial_marketing$ad_name <- ReplaceAdnamefunction(imperial_marketing$ad_name)

  # Save --------
  
  imperial_marketing[is.na(imperial_marketing)] <- 0
  
  imperial_marketing$installs_csv <- imperial_marketing$installs
  imperial_marketing$installs <- imperial_marketing$installs.x
  imperial_marketing$money <- imperial_marketing$money_999
  imperial_marketing$payers<- imperial_marketing$payer_999
  setwd("C:/Users/user/Google ????/09.2017/??????? ????????")
  #setwd("/Users/cu037/Google Drive/09.2017/?????????????? ????????????????")
  write.table( 
    x = imperial_marketing,
    file = "imperial_marketing.txt",
    sep = ";",
    row.names = F,
    quote = F
  )
  
  write.csv(imperial_marketing,'imperial_marketing.csv',row.names = F)
  
  
  
  # ----
  #first commit