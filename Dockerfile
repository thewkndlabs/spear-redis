FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY spearedis.csproj ./
RUN dotnet restore spearedis.csproj

COPY . ./
RUN dotnet publish spearedis.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish ./

ENV ASPNETCORE_URLS=http://+:8080 \
  REDIS_PROXY_PORT=9379

EXPOSE 8080
EXPOSE 9379

ENTRYPOINT ["dotnet", "spearedis.dll"]
