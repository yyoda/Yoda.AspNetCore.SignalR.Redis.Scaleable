FROM microsoft/dotnet:2.1-aspnetcore-runtime AS base
WORKDIR /app
EXPOSE 80

FROM microsoft/dotnet:2.1-sdk AS build
WORKDIR /src
COPY SignalR.Server/SignalR.Server.csproj SignalR.Server/
RUN dotnet restore SignalR.Server/SignalR.Server.csproj
COPY . .
WORKDIR /src/SignalR.Server
RUN dotnet build SignalR.Server.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish SignalR.Server.csproj -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "SignalR.Server.dll"]
